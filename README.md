# TinyLSM

I built this while reading Designing Data-Intensive Applications to get a better feel for how storage engines actually work. It's an LSM-tree written in Python with SSTables, Bloom filters, leveled compaction, sparse indexing, CRC checksums, atomic manifest writes, concurrent reads via a read-write lock, and snapshot reads via MVCC.

## How to Run

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
python -m src.main       # run the REPL
python -m src.benchmark  # run benchmarks
pytest tests/            # run test suite
```

Once running, the REPL accepts the following commands:

```
SET key value      # write a key-value pair
GET key            # read a value by key
DELETE key         # delete a key
SCAN key1 key2     # return all keys in the range [key1, key2]
STATS              # print store statistics
EXIT               # quit
```

Configuration is done through a `.env` file in the project root:

```
LOG_FILE_NAME="log_file.txt"       # WAL file name
MAX_MEMTABLE_SIZE=4096             # memtable size in bytes before flush (default 4KB)
MAX_L0_FILES=4                     # L0 SSTable count before compaction triggers
BLOOM_FILTER_SIZE=1000             # number of bits in each bloom filter
HASH_FUNCTIONS=5                   # number of hash functions used by bloom filter
SPARSE_INDEX_N=4                   # sample every Nth key for the sparse index
WAL_BUFFER_SIZE=100                # number of writes before WAL is flushed to disk
TOMBSTONE_VALUE="__TOMBSTONE__"
BENCHMARK_N=100000                 # number of operations to run in the benchmark
```

## Architecture

### Memtable
Writes go into an in-memory dictionary first. Since there is no disk I/O on the write path, the writes are fairly quick. Once the memtable hits `MAX_MEMTABLE_SIZE` bytes, it gets flushed to disk as an SSTable.

### Write-Ahead Log (WAL)
Every write goes to the WAL and memtable together. WAL writes are buffered and flushed every WAL_BUFFER_SIZE operations, with a forced flush before any memtable hits disk. On startup, the log is replayed to recover any writes that hadn't been flushed yet.

### SSTables
When the memtable flushes, keys are sorted and written to a new file. Reads binary search instead of scanning. 

### Bloom Filters
Each SSTable has a bloom filter. Before reading an SSTable for a key, the filter is checked first. If it says the key isn't there, the file read is skipped entirely. Lookups for missing keys stay fast regardless of how many SSTables are on disk. 

### Sparse Index
Each SSTable has a sparse index: a sampled list of keys and their byte offsets, recorded every N entries. On lookup the sparse index is binary searched to find the closest offset, then the file seek jumps directly to that point. 

### Leveled Compaction
SSTables are organized into levels. All flushes land in L0, where files can have overlapping key ranges. When L0 hits `MAX_L0_FILES`, it compacts into L1 by merging with any overlapping L1 files. Each level is 10x larger than the last, so if L1 overflows, then it cascades down. A manifest file (`manifest.json`) tracks each SSTable's level and key range.

### Stats API
The `stats()` method returns a snapshot of the store's current state:

- `sstable_count`: total number of SSTables across all levels
- `sstables_per_level`: SSTable count broken down by level
- `total_size_bytes`: total size of all SSTable files on disk
- `memtable_size_bytes`: current memtable size in bytes
- `memtable_keys`: number of live keys currently in the memtable

### MVCC (Snapshot Reads)
Every write is assigned a monotonically increasing sequence number. The memtable stores all versions of each key as a list of `(seq, value)` pairs. `get(key)` returns the latest version by default. `get(key, at=seq)` returns the most recent version where the sequence number is at or below `seq`. All versions are written to disk on flush, so snapshot reads work across SSTable boundaries too.

### Tombstones
Deletes write a tombstone marker instead of removing data immediately, since the key might exist in an older SSTable. The tombstone gets carried through compaction until cleanup is implemented.

### CRC Checksums
Each SSTable line is written with a CRC32 checksum. On read, the checksum is recomputed and if it doesn't match a `ValueError` is raised right away instead of returning bad data.

### Atomic Manifest Writes
The manifest is written to a temp file and renamed into place with `os.replace`, which is atomic on both Windows and Linux. A crash mid-write can't corrupt it.

### Concurrent Reads
A read-write lock lets multiple `get` and `scan` calls run in parallel while writes stay exclusive. SSTable reads release the GIL during file I/O, so threads actually overlap on disk reads.

## Benchmarks

Run with `python -m src.benchmark`. These results are from my personal Windows 11 machine with a 4KB memtable and N=100,000:

| Operation          | Ops/sec |
|--------------------|---------|
| Writes             | ~4,000  |
| Reads (1 thread)   | ~7,000  |
| Reads (4 threads)  | ~8,000  |
| Misses             | ~18,000 |

Misses are faster than hits because bloom filters skip the SSTable read entirely for keys that don't exist. I used 4 threads since performance stops improving past that point. The gain from 1 to 4 threads is real but modest, past 4, the GIL overhead starts eating into whatever parallelism the I/O would give you.