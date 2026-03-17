# TinyLSM

This is a project I've worked on while reading Designing Data-Intensive Applications to help me understand the concepts in the book better. This is a LSM-tree (Log-Structured Merge-tree) engine written in Python that uses SSTables, bloom filters, compaction, sparse indexing, and more. Writes are buffered in memory, while the data integrity is preserved via the use of the hard drive. Data is flushed to sorted files on disk (SSTables), which are merged together through a compaction process to remove obsolete data from past operations and optimize the performance of reads from the database.

## How to Run

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
cd src
python main.py
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
LOG_FILE_NAME=log_file.txt
MAX_ENTRIES=10
MAX_SSTABLES=20
BLOOM_FILTER_SIZE=1000
HASH_FUNCTIONS=5
SPARSE_INDEX_N=4
```

## Architecture

### Memtable
Writes go into an in-memory dictionary first. This keeps writes fast — no disk I/O on the write path. Once the memtable hits `MAX_ENTRIES`, it gets flushed to disk as an SSTable.

### Write-Ahead Log (WAL)
Every write is appended to a log file before touching memory. If the process crashes, the log is replayed on startup to rebuild the memtable. Once the memtable is flushed, the log is cleared since the data is now in an SSTable.

### SSTables
When the memtable is flushed, keys are sorted and written to a new file (`sst_1`, `sst_2`, etc.). These files are immutable — they're never modified, only replaced during compaction. Because keys are sorted, lookups can use binary search rather than scanning the whole file.

### Bloom Filters
Each SSTable has a corresponding bloom filter. Before searching an SSTable for a key, the bloom filter is checked first. If the filter says the key definitely isn't there, the file read is skipped entirely. This makes lookups for non-existent keys much cheaper, especially as the number of SSTables grows.

### Sparse Index
Each SSTable also has a sparse index — a sampled list of keys and their byte offsets in the file, recorded every N entries. On lookup, the sparse index is binary searched to find the closest offset, and the file is seeked to that position directly. This avoids loading the entire SSTable into memory just to find one key.

### Compaction
Over time, SSTables accumulate. Compaction merges all of them into one, keeping only the most recent value for each key and dropping deleted entries (tombstones). This keeps read performance from degrading and reclaims disk space.

### Tombstones
Deletes don't immediately remove data — they write a special tombstone marker. This is necessary because the key might exist in an older SSTable on disk. The tombstone propagates through compaction, at which point it's dropped entirely.