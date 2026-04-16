# TinyLSM

TinyLSM is a small Python key-value store built to explore LSM-tree storage ideas in code. It started as a learning project while reading Designing Data-Intensive Applications, and the repo now includes both a local storage engine and a simple replicated HTTP cluster built on top of it.

It is not meant to be a production database. It is meant to be readable, hackable, and useful for learning how the pieces fit together.

## What is implemented

- Write-ahead logging
- Mutable and immutable memtables
- Background flush to SSTables
- Bloom filter sidecars
- Sparse index sidecars
- Leveled compaction
- CRC32 checksums on SSTable records
- Atomic manifest writes with `os.replace`
- Snapshot reads with sequence numbers
- Concurrent reads with a read-write lock
- FastAPI-based multi-node replication with leader election, majority write acknowledgement, follower catch-up, and log snapshots

## Requirements

- Python 3.11 or newer

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
pytest tests
```

Run the local REPL:

```bash
python -m src.main
```

Run the benchmark script:

```bash
python -m src.benchmark
```

The standalone store writes data files into the current working directory. For a clean run, use an empty folder or clear old `sst_*`, `manifest.json`, and WAL files before starting again.

## REPL Commands

Once `python -m src.main` is running, these commands are available:

```text
SET key value
GET key
DELETE key
SCAN start_key end_key
STATS
EXIT
```

Keys and values are currently space-delimited, so the REPL works best with single-token keys and values.

## Cluster Mode

Each node runs a FastAPI server from `src.cluster.node`. Start a 3-node cluster in three terminals:

```bash
python -m src.cluster.node 8000 node_data_8000 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
python -m src.cluster.node 8001 node_data_8001 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
python -m src.cluster.node 8002 node_data_8002 http://localhost:8000 http://localhost:8000,http://localhost:8001,http://localhost:8002
```

Arguments:

`<port> <data_dir> <leader_url> <comma_separated_node_urls>`

Notes:

- The third argument is the node to treat as leader on startup.
- If that leader goes away, the remaining nodes elect a new leader.
- Writes can be sent to any node. Followers forward them to the leader.
- A write succeeds only after a majority of nodes acknowledge it.
- A consistent read is forwarded to the leader.

### HTTP Endpoints

- `POST /set` with `{"key": "foo", "value": "bar"}`
- `POST /delete` with `{"key": "foo"}`
- `GET /get?key=foo`
- `GET /get?key=foo&consistent=true`
- `GET /status`
- `POST /add_node` with `{"node_url": "http://localhost:8003"}`
- `POST /remove_node` with `{"node_url": "http://localhost:8003"}`

Cluster nodes persist their own files inside the `data_dir` you pass at startup.

## Configuration

Configuration is loaded from a `.env` file in the project root.

| Variable | Default | Purpose |
|---|---:|---|
| `LOG_FILE_NAME` | `log_file.txt` | WAL file for the standalone store |
| `MAX_MEMTABLE_SIZE` | `4096` | Flush threshold in bytes |
| `TOMBSTONE_VALUE` | `__TOMBSTONE__` | Delete marker |
| `HASH_FUNCTIONS` | `5` | Hash count for bloom filters |
| `BLOOM_FILTER_SIZE` | `5` | Bloom filter size in bits |
| `SPARSE_INDEX_N` | `4` | Record every Nth key in the sparse index |
| `MAX_L0_FILES` | `2` | Number of L0 files before compaction kicks in |
| `BENCHMARK_N` | `100000` | Number of benchmark operations |
| `WAL_BUFFER_SIZE` | `100` | WAL flush interval in operations |
| `LOG_COMPACTION_THRESHOLD` | `10000` | Cluster log length before snapshotting |

The checked-in defaults are intentionally small so flushes, compactions, and tests happen quickly. For real experiments, you will probably want larger bloom filters and larger level thresholds.

## How the Store Works

### Write Path

Every write is appended to the WAL and applied to the active memtable. Once the active memtable grows past `MAX_MEMTABLE_SIZE`, it is rotated into an immutable memtable, a fresh memtable becomes active immediately, and a background thread flushes the immutable one to a new SSTable.

### Read Path

Reads check the active memtable first, then the immutable memtable, then SSTables. SSTable lookups use:

- Manifest key ranges to skip unrelated files
- Bloom filters to avoid unnecessary file reads
- Sparse indexes to seek close to the target key before scanning

### SSTables and Compaction

Each SSTable record stores `key seq value checksum`. The checksum is verified on read. L0 files may overlap. When enough L0 files build up, they are compacted into the next level along with overlapping files there. During compaction, overwritten versions are dropped and tombstones are removed once older data can no longer resurface from a lower level.

### Snapshot Reads

Each write gets a monotonically increasing sequence number. The Python API supports snapshot reads:

```python
store.get("foo", at=seq)
store.scan("a", "z", at=seq)
```

That lets you read the latest value at or before a specific sequence number.

### Startup and Recovery

On startup, the standalone store reloads existing SSTables, bloom filters, sparse indexes, and then replays the WAL. The manifest is stored in `manifest.json` and written atomically through a temporary file plus `os.replace`.

## How the Cluster Works

The cluster layer wraps the storage engine with a small HTTP service. The protocol is Raft-inspired rather than a full Raft implementation.

- One node acts as leader at a time.
- Followers receive heartbeats from the leader.
- If heartbeats stop, followers start an election after a randomized timeout.
- The leader applies a write locally, appends it to `replication.log`, and sends it to followers in parallel.
- Followers that miss entries can catch up through heartbeats or through `/sync` on startup.
- Once the replication log grows past `LOG_COMPACTION_THRESHOLD`, the node snapshots state to `snapshot.json` and truncates the log.

Each node also persists election state in `state.json`.

## Benchmarks

Run:

```bash
python -m src.benchmark
```

The benchmark script creates a temporary store, runs writes, single-threaded reads, 4-thread reads, and misses, then prints ops/sec for your machine.

## Tests

Run the full test suite with:

```bash
pytest tests
```

The tests cover:

- Basic set, get, delete, scan, and iteration
- WAL replay and restart behavior
- Compaction and tombstone handling
- Snapshot reads
- Checksum and manifest durability cases
- Concurrent reads
- Cluster replication, forwarding, elections, restart recovery, snapshots, and membership changes

## Files You Will See

Standalone store files:

- `log_file.txt`
- `manifest.json`
- `sst_<n>`
- `sst_<n>.index`
- `sst_<n>.bloom`

Cluster node files:

- `replication.log`
- `snapshot.json`
- `state.json`

## Limitations

- Keys and values are treated as plain strings.
- The REPL and WAL format do not safely encode values with spaces.
- The cluster protocol is intentionally small and simplified.
- There is no authentication, encryption, or production hardening.
