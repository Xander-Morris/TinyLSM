# TinyLSM

A project I built while reading Designing Data-Intensive Applications. It's an LSM-tree (Log-Structured Merge-tree) storage engine written in Python, implementing SSTables, Bloom filters, leveled compaction, sparse indexing, CRC checksums, atomic manifest writes, and concurrent reads via a read-write lock.

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
Writes go into an in-memory dictionary first, which keeps writes fast with no disk I/O on the write path. Once the memtable exceeds `MAX_MEMTABLE_SIZE` bytes, it gets flushed to disk as an SSTable.

### Write-Ahead Log (WAL)
Every write is appended to the WAL and the memtable in sequence. WAL writes are buffered and flushed to disk every WAL_BUFFER_SIZE operations for throughput, with a full flush forced before any memtable is persisted to an SSTable. On startup, the log is replayed to rebuild any unflushed memtable state.

### SSTables
When the memtable is flushed, keys are sorted and written to a new file. These files are immutable and never modified after creation, only replaced during compaction. Because keys are sorted, lookups can use binary search rather than scanning the whole file.

### Bloom Filters
Each SSTable has a corresponding bloom filter. Before searching an SSTable for a key, the bloom filter is checked first. If it says the key definitely isn't there, the file read is skipped entirely. This makes lookups for non-existent keys much cheaper as the number of SSTables grows.

### Sparse Index
Each SSTable has a sparse index: a sampled list of keys and their byte offsets in the file, recorded every N entries. On lookup, the sparse index is binary searched to find the nearest offset and the file is seeked directly to that position, avoiding loading the entire SSTable into memory.

### Leveled Compaction
SSTables are organized into levels. L0 is where all flushes land, and files there can have overlapping key ranges. When L0 hits `MAX_L0_FILES`, a compaction is triggered that merges all L0 files with any overlapping L1 files, producing new L1 files with non-overlapping key ranges. Each level is 10x larger than the previous, so if L1 exceeds its limit after a compaction the process cascades down to L2, and so on. A manifest file (`manifest.json`) tracks which SSTables exist, what level they belong to, and their key range.

### Tombstones
Deletes don't immediately remove data. Instead they write a tombstone marker, which is necessary because the key might exist in an older SSTable on disk. The tombstone propagates through compaction, at which point it's dropped entirely.

### CRC Checksums
Every SSTable line is written with a CRC32 checksum. On read, the checksum is recomputed and compared. If they don't match, a `ValueError` is raised immediately rather than returning corrupt data silently.

### Atomic Manifest Writes
The manifest is written to a temporary file and then renamed into place with `os.replace`, which is atomic on both Windows and Linux. A crash mid-save can't leave the manifest in a partially-written state.

### Concurrent Reads
A read-write lock allows multiple `get` and `scan` calls to run in parallel while writes remain exclusive. SSTable reads involve file I/O, which causes Python to release the GIL, so concurrent reads can genuinely run in parallel.

## Benchmarks

Run with `python -m src.benchmark`. Results on a personal Windows 11 machine with a 4KB memtable and N=100,000, which triggers real flushes and compactions on every run:

| Operation          | Ops/sec |
|--------------------|---------|
| Writes             | ~4,000  |
| Reads (1 thread)   | ~7,000  |
| Reads (4 threads)  | ~8,000  |
| Misses             | ~18,000 |

Miss lookups are faster than hits because bloom filters skip the SSTable entirely for keys that don't exist. Concurrent reads are faster than single-threaded because SSTable file I/O releases the GIL, allowing threads to genuinely overlap. The WAL write buffer trades a small crash-recovery window for better write throughput.
