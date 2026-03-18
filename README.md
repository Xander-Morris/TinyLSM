# TinyLSM

This is a project I've worked on while reading Designing Data-Intensive Applications to help me understand the concepts in the book better. This is an LSM-tree (Log-Structured Merge-tree) engine written in Python that uses SSTables, Bloom filters, level compaction, sparse indexing, and more. Writes are buffered in memory, while data integrity is preserved via the write-ahead log. Data is flushed to sorted files on disk (SSTables), which are organized into levels and merged through a compaction process to remove obsolete data and optimize read performance.

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
BENCHMARK_N=10000                  # number of operations to run in the benchmark
```

## Architecture

### Memtable
Writes go into an in-memory dictionary first. This keeps writes fast — no disk I/O on the write path. Once the memtable exceeds `MAX_MEMTABLE_SIZE` bytes, it gets flushed to disk as an SSTable.

### Write-Ahead Log (WAL)
Every write is appended to the WAL and the memtable in sequence. WAL writes are buffered and flushed to disk every WAL_BUFFER_SIZE operations for throughput, with a full flush forced before any memtable is persisted to an SSTable. On startup, the log is replayed to rebuild any unflushed memtable state.

### SSTables
When the memtable is flushed, keys are sorted and written to a new file. These files are immutable — they're never modified after creation, only replaced during compaction. Because keys are sorted, lookups can use binary search rather than scanning the whole file.

### Bloom Filters
Each SSTable has a corresponding bloom filter. Before searching an SSTable for a key, the bloom filter is checked first. If the filter says the key definitely isn't there, the file read is skipped entirely. This makes lookups for non-existent keys much cheaper as the number of SSTables grows.

### Sparse Index
Each SSTable also has a sparse index — a sampled list of keys and their byte offsets in the file, recorded every N entries. On lookup, the sparse index is binary searched to find the nearest offset, and the file is seeked to that position directly. This avoids loading the entire SSTable into memory just to find one key.

### Leveled Compaction
SSTables are organized into levels. L0 is where all flushes land and files here can have overlapping key ranges. When L0 hits `MAX_L0_FILES`, a compaction is triggered that merges all L0 files with any overlapping L1 files, producing new L1 files with non-overlapping key ranges. Each level is 10x larger than the previous — if L1 exceeds its limit after a compaction, the process cascades down to L2, and so on. A manifest file (`manifest.json`) tracks which SSTables exist, what level they belong to, and their key range.

### Tombstones
Deletes don't immediately remove data — they write a special tombstone marker. This is necessary because the key might exist in an older SSTable on disk. The tombstone propagates through compaction, at which point it's dropped entirely.

## Benchmarks

Run with `python -m src.benchmark`. Results on a Windows 11 machine with a 4KB memtable and N=10,000, which triggers real flushes and compactions on every run:

| Operation | Ops/sec |
|-----------|---------|
| Writes    | ~8,400  |
| Reads     | ~8,900  |
| Misses    | ~49,000 |

Miss lookups are ~10x faster than reads — bloom filters eliminate file I/O entirely for keys that don't exist. The WAL uses a write buffer of 100 entries before flushing to disk, trading a small crash-recovery window for higher write throughput.