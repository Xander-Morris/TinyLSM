# TinyLSM

I built this while reading Designing Data-Intensive Applications to get a better feel for how storage engines actually work. It's an LSM-tree written in Python with SSTables, Bloom filters, leveled compaction, sparse indexing, CRC checksums, atomic manifest writes, concurrent reads via a read-write lock, snapshot reads via MVCC, and an immutable memtable for non-blocking flushes. It also includes a distributed key-value layer built on top of the storage engine, with leader/follower replication, quorum writes, follower catch-up sync, and a persistent replication log that survives leader restarts.

## How to Run

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
python -m src.main       # run the REPL
python -m src.benchmark  # run benchmarks
pytest tests/            # run test suite
```

To run a 3-node cluster locally:

```bash
# In three separate terminals:
python -m src.cluster.node 8000 data/node0 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
python -m src.cluster.node 8001 data/node1 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
python -m src.cluster.node 8002 data/node2 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
```

Arguments: `<port> <data_dir> <leader_url> <comma_separated_node_urls>`

The first node is the leader. Writes go to any node and are forwarded to the leader, which replicates to all followers and requires a majority to acknowledge before returning success.

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
Writes go into an in-memory dictionary first. Since there is no disk I/O on the write path, the writes are fairly quick. Once the memtable hits `MAX_MEMTABLE_SIZE` bytes, it gets promoted to an immutable memtable and a fresh active memtable is opened immediately. The immutable memtable is then flushed to disk as an SSTable. Reads check the active memtable first, then the immutable memtable if one exists, then SSTables.

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
- `bytes_written_disk`: total bytes written to SSTable files across all flushes and compactions
- `write_amplification`: ratio of bytes written to disk vs bytes written by the caller — compaction rewrites data across levels, so this grows over time

### MVCC (Snapshot Reads)
Every write is assigned a monotonically increasing sequence number. The memtable stores all versions of each key as a list of `(seq, value)` pairs. `get(key)` returns the latest version by default. `get(key, at=seq)` returns the most recent version where the sequence number is at or below `seq`. All versions are written to disk on flush, so snapshot reads work across SSTable boundaries too.

### Tombstones
Deletes write a tombstone marker instead of removing data immediately, since the key might exist in an older SSTable. During compaction, tombstones are dropped once there are no files at a lower level that could have the original value. Old versions of a key are also dropped during compaction, only the latest version survives.

### Distributed Key-Value Layer

The LSM-tree is wrapped in a FastAPI HTTP server. A cluster is a fixed set of nodes with one designated leader. The leader accepts writes, applies them locally, then replicates to all followers. A write is acknowledged to the caller only after a majority of nodes confirm it, so the cluster can tolerate up to `floor(n/2)` node failures without losing writes. Followers that are behind can request the full operation history from the leader via `/sync` and replay it to catch up.

A write to a follower is forwarded transparently to the leader, so the caller does not need to know which node is the leader.

### Persistent Replication Log

Every write the leader processes is appended to an on-disk replication log (`replication.log`) in the leader's data directory. On startup, the leader reads this file back and reconstructs its in-memory log before accepting connections. This means a new or restarted follower can always sync the complete history from the leader, even after the leader has been restarted. Without this, a leader restart would wipe the in-memory log and leave any new followers unable to catch up to writes that happened before the restart.

### CRC Checksums
Each SSTable line is written with a CRC32 checksum. On read, the checksum is recomputed and if it doesn't match a `ValueError` is raised right away instead of returning bad data.

### Atomic Manifest Writes
The manifest is written to a temp file and renamed into place with `os.replace`, which is atomic on both Windows and Linux. A crash mid-write can't corrupt it.

### Concurrent Reads
A read-write lock lets multiple `get` and `scan` calls run in parallel while writes stay exclusive. SSTable reads release the GIL during file I/O, so threads actually overlap on disk reads.

## Benchmarks

Run with `python -m src.benchmark`. These results are from my personal Windows 11 machine with a 4KB memtable and N=100,000:

| Operation          | Ops/sec   |
|--------------------|-----------|
| Writes             | ~262,000  |
| Reads (1 thread)   | ~407,000  |
| Reads (4 threads)  | ~101,000  |
| Misses             | ~94,000   |

Writes are fast because the background flush thread means a write just hits the WAL buffer and the memtable dict, then returns. Disk I/O happens in the background without blocking the caller.

Reads are fast for the same reason. With writes that quick, most data is still in the memtable by the time reads run, so reads are mostly dictionary lookups.

4-thread reads are actually slower than single-threaded here. When reads hit the memtable, the work is CPU-bound, not I/O-bound. Python's GIL forces threads to take turns on CPU work, so the switching overhead makes things worse. Multi-threading helps when threads block on disk I/O and can genuinely overlap. With memtable hits, they can't.

Misses are slower than reads because each miss has to check the bloom filter for every SSTable on disk before confirming the key doesn't exist. A memtable hit returns immediately.