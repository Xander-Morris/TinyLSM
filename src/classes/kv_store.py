"""The durable, local LSM-tree key-value store used by TinyLSM.

``KVStore`` keeps recent writes in memory, records them in a write-ahead log,
and flushes immutable memtables to sorted-string tables (SSTables).  It also
owns recovery, compaction, and the snapshot-pinning rules that preserve
historical versions while a caller is reading them.
"""

import glob
import heapq
import json
import os
import threading
from collections import Counter
from contextlib import contextmanager

import src.classes.bloom_filter as bloom_filter
import src.classes.manifest as manifest
import src.classes.read_write_lock as read_write_lock
import src.config as config
from src.classes.tombstone import TombstoneType
from src.utils.file_lock import try_lock_fd, unlock_fd
from src.utils.sstable import (
    sst_index,
    binary_search,
    write_to_sstable_file,
    read_sstable_tuples,
    iter_sstable_from,
    search_sstable_with_index,
    cleanup_orphan_sst_files,
)
from src.utils.sparse_index import load_sparse_index
from src.utils.wal import parse_wal_record, format_wal_record, load_wal
from src.utils.memtable import memtable_iter, get_raw_value_from_table_at
from src.utils.compaction import chunk_by_target_size

_TOMBSTONE = TombstoneType()
_TOMBSTONE_BYTES = 1  # Accounting weight for tombstone marker in memtable

class KVStore:
    """A small, thread-safe key-value store backed by an LSM tree.

    Args:
        data_dir: Directory that owns the WAL, manifest, SSTables, and lock
            file.  The current working directory is used when omitted.

    A store permits multiple concurrent readers but only one writer at a time.
    It also permits one process per data directory, enforced by ``LOCK``.
    """

    def __init__(self, data_dir=None):
        """Open ``data_dir``, recover published data, and acquire its lock."""
        self._data_dir = os.path.abspath(data_dir) if data_dir else os.path.abspath(os.getcwd())
        os.makedirs(self._data_dir, exist_ok=True)

        # Acquire process-level exclusive lock on this data directory.
        # Another process holding the lock = refuse to open (would corrupt).
        self._lock_fh = None
        try:
            self._lock_fh = open(self._path("LOCK"), 'a+b')
            self._lock_fh.seek(0)
        except OSError as exc:
            if self._lock_fh is not None:
                self._lock_fh.close()
                self._lock_fh = None
            raise RuntimeError(f"Could not open lock file for {self._data_dir}") from exc
        if not try_lock_fd(self._lock_fh.fileno()):
            self._lock_fh.close()
            self._lock_fh = None
            raise RuntimeError(f"Another process holds the lock on {self._data_dir}")

        self._store = {}
        self._active_snapshots = Counter()
        self._imm_memtable = None
        self._imm_entries = 0
        self._entries = 0
        self._index_counter = 0
        self._wal_buffer_count = 0
        self._bytes_written_disk = 0
        self._bytes_written_user = 0
        self._seq = 0
        self._wal_counter_index = 1 # current index for wal file name, like wal-1.log if index is 1
        self._bloom_filters = {}
        self._sparse_indexes = {}
        self._flush_thread = None
        self._flush_drained = threading.Condition()

        try:
            self._manifest = manifest.Manifest.load(self._data_dir)
            self._lock = read_write_lock.ReadWriteLock()
            self._cleanup_orphan_sst_files()
            self._load_meta()
            self._load_sstables()
            load_wal(self._path)

            try:
                with open(self._path(config.LOG_FILE_NAME), 'r', encoding='utf-8') as file:
                    for line in file:
                        if not self._replay_line(line):
                            break
            except FileNotFoundError:
                pass

            self._wal = open(self._path(config.LOG_FILE_NAME), 'a', encoding='utf-8')
        except Exception:
            self._release_directory_lock()
            raise

    @contextmanager
    def snapshot(self):
        """Pin and yield the sequence number for a consistent read snapshot.

        Pass the yielded sequence to :meth:`get`, :meth:`scan`, or
        :meth:`iter`.  While this context is open, compaction preserves the
        versions needed to answer reads at that sequence.  Once the context
        closes, a later compaction may reclaim that older history.

        Yields:
            int: The newest committed sequence number visible to the snapshot.
        """
        with self._lock.write():
            seq = self._seq
            self._active_snapshots[seq] += 1

        try:
            yield seq
        finally:
            # This snapshot is no longer using that sequence.
            with self._lock.write():
                self._active_snapshots[seq] -= 1
                if self._active_snapshots[seq] == 0:
                    del self._active_snapshots[seq]

    def _oldest_active_snapshot_seq(self):
        """Return the oldest pinned sequence, or ``None`` when none exist.

        Keeping data for the oldest active snapshot automatically keeps enough
        history for every newer active snapshot as well.
        """
        if not self._active_snapshots:
            return None
        return min(self._active_snapshots)

    def _versions_to_keep(self, versions, oldest_snapshot):
        """Select the minimal versions that remain readable after compaction.

        A snapshot at sequence ``S`` needs the latest version at or before
        ``S`` plus every later version.  Versions older than that baseline can
        no longer be observed by any active snapshot and may be discarded.
        """
        versions = sorted(versions, key=lambda item: item[0])

        if oldest_snapshot is None:
            return [versions[-1]]

        baseline = None
        newer_versions = []

        for seq, value in versions:
            if seq <= oldest_snapshot:
                baseline = (seq, value)
            else:
                newer_versions.append((seq, value))

        return ([baseline] if baseline is not None else []) + newer_versions

    def _path(self, name):
        """Return the absolute path for a file managed by this store."""
        return os.path.join(self._data_dir, name)

    def _release_directory_lock(self):
        """Release the process-level lock if this instance still owns it."""
        if self._lock_fh is not None:
            unlock_fd(self._lock_fh.fileno())
            self._lock_fh.close()
            self._lock_fh = None

    def _meta_path(self):
        """Return the path used for persistent counters and sequence state."""
        return self._path("meta")

    def _load_meta(self):
        """Load nonessential persisted counters when a valid meta file exists."""
        try:
            with open(self._meta_path(), 'r', encoding='utf-8') as file:
                meta = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return

        try:
            self._seq = int(meta.get("seq", 0))
            self._bytes_written_disk = int(meta.get("bytes_written_disk", 0))
            self._bytes_written_user = int(meta.get("bytes_written_user", 0))
            self._wal_counter_index = int(meta.get("wal_counter_index", 1))
        except (TypeError, ValueError):
            # Corrupt counters — fall back to defaults rather than crash init.
            self._seq = 0
            self._bytes_written_disk = 0
            self._bytes_written_user = 0
            self._wal_counter_index = 1

    def _save_meta(self):
        """Atomically persist sequence and write-accounting counters."""
        meta = {
            "seq": self._seq,
            "bytes_written_disk": self._bytes_written_disk,
            "bytes_written_user": self._bytes_written_user,
            "wal_counter_index": self._wal_counter_index,
        }
        tmp = self._path("meta.tmp")
        with open(tmp, 'w', encoding='utf-8') as file:
            json.dump(meta, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp, self._meta_path())

    def _cleanup_orphan_sst_files(self):
        """Delete SSTable sidecars not referenced by the current manifest.

        A crash before a manifest update can leave fully written but unpublished
        files behind.  The manifest is the source of truth for published data.
        """
        expected = {sst_index(entry) for entry in self._manifest.entries}
        cleanup_orphan_sst_files(self._path, expected)

    def _wal_write_record(self, op, seq, key, value=None):
        """Append one checksummed mutation record to the open WAL."""
        self._wal.write(format_wal_record(op, seq, key, value))
        self._increment_wal_buffer_count()

    def _increment_wal_buffer_count(self):
        """Synchronize the WAL whenever the configured group size is reached."""
        self._wal_buffer_count += 1

        if self._wal_buffer_count >= config.WAL_BUFFER_SIZE:
            self._wal.flush()
            os.fsync(self._wal.fileno())
            self._wal_buffer_count = 0

    def _replay_line(self, line):
        """Replay one valid WAL line, returning ``False`` for a damaged record."""
        record = parse_wal_record(line)
        if record is None:
            return False
        seq = int(record["seq"])
        if seq > self._seq:
            self._seq = seq
        key = record["key"]
        if record["op"] == "SET":
            self._set_key_seq_value(key, record["value"], seq)
        elif record["op"] == "DELETE":
            self._set_key_seq_value(key, _TOMBSTONE, seq)
        return True

    def _write_sstable(self, index, data):
        """Write an SSTable, then publish its in-memory search sidecars."""
        write_result = write_to_sstable_file(self._path, index, data)
        self._sparse_indexes[index] = write_result[0]
        self._write_bloom_filter(data, index)
        self._bytes_written_disk += os.path.getsize(self._path(f"sst_{index}"))

        return write_result

    def _write_bloom_filter(self, items, index):
        """Build and persist the bloom filter used to skip SSTable reads."""
        self._bloom_filters[index] = bloom_filter.write_bloom_filter(
            self._path, index, items, config.BLOOM_FALSE_POSITIVE_RATE
        )

    def _compact_level(self, level):
        """Merge one level into the next while preserving pinned snapshots.

        The caller holds the store write lock.  This makes the active snapshot
        set stable while versions are selected and the manifest is replaced.
        """
        entries = [entry for entry in self._manifest.entries if entry["level"] == level]

        if not entries:
            return

        overall_min = min(entry["min_key"] for entry in entries)
        overall_max = max(entry["max_key"] for entry in entries)
        next_entries = [entry for entry in self._manifest.entries if entry["level"] == level + 1 and entry["min_key"] <= overall_max and entry["max_key"] >= overall_min]
        merged = {}

        def read_from_entries_list(entries_list):
            """Add every version from one compaction input set to ``merged``."""
            for entry in entries_list:
                index = sst_index(entry)

                for key, seq, value in read_sstable_tuples(self._path, index):
                    if key not in merged:
                        merged[key] = []
                    merged[key].append((seq, value))

        read_from_entries_list(next_entries)
        read_from_entries_list(entries)

        oldest_snapshot = self._oldest_active_snapshot_seq()
        compacted = {}
        for key, versions in merged.items():
            kept_versions = self._versions_to_keep(versions, oldest_snapshot)

            # Without a pinned snapshot, the newest tombstone may be dropped
            # only if no lower level can still contain an older value.  With a
            # pinned snapshot, tombstones are data too: an old read may need to
            # observe either the tombstone or the value before it.
            if oldest_snapshot is None and kept_versions[0][1] is _TOMBSTONE:
                has_older = any(
                    entry["level"] > level + 1
                    and entry["min_key"] <= key <= entry["max_key"]
                    for entry in self._manifest.entries
                )
                if not has_older:
                    continue

            compacted[key] = kept_versions

        merged = sorted(compacted.items())

        """
            Step 1: write all new SST files (data, bloom, index) durably to disk
            BEFORE touching the manifest or deleting old files. Crash before
            manifest update = orphan new files cleaned on next boot.
        """
        target_sstable_size = config.MAX_MEMTABLE_SIZE * (10 ** (level + 1))
        new_entries = []
        for chunk in chunk_by_target_size(merged, target_sstable_size):
            self._index_counter += 1
            new_idx = self._index_counter
            self._write_sstable(new_idx, chunk)
            new_entries.append((level + 1, f"sst_{new_idx}", chunk[0][0], chunk[-1][0]))

        # Step 2: single atomic manifest update - add new and remove old together.
        for lvl, fname, mink, maxk in new_entries:
            self._manifest.add(lvl, fname, mink, maxk)
        for entry in entries + next_entries:
            self._manifest.remove(entry["file_name"])
        self._manifest.save()

        # Step 3: delete old files. Crash here = orphan old files cleaned on boot.
        for entry in entries + next_entries:
            index = sst_index(entry)
            for ext in ("", ".bloom", ".index"):
                try:
                    os.remove(self._path(f"sst_{index}{ext}"))
                except FileNotFoundError:
                    pass
            self._bloom_filters.pop(index, None)
            self._sparse_indexes.pop(index, None)

    def _compact(self):
        """Compact levels until the next level is below its file-count limit."""
        level = 0

        while True:
            self._compact_level(level)
            next_count = sum(1 for entry in self._manifest.entries if entry["level"] == level + 1)
            level_limit = config.MAX_L0_FILES * (10 ** (level + 1))

            if next_count < level_limit:
                break

            level += 1

    def _flush(self):
        """Rotate the mutable memtable and flush it on a background thread."""
        if self._imm_memtable is not None:
            return

        self._index_counter += 1
        index = self._index_counter
        self._imm_memtable = self._store
        self._imm_entries = self._entries
        self._store = {}
        self._entries = 0
        self._wal.flush()
        os.fsync(self._wal.fileno())
        self._wal.close()
        wal_file_name = f"wal-{self._wal_counter_index}.log"
        self._wal_counter_index += 1 # increment index for file name of next WAL that is written to
        os.rename(self._path(config.LOG_FILE_NAME), self._path(wal_file_name))
        self._wal = open(self._path(config.LOG_FILE_NAME), 'a', encoding='utf-8')

        def _threaded_funct():
            """Write the immutable memtable, then publish it under the write lock."""
            try:
                if not self._imm_memtable:
                    return
                sorted_store = sorted(self._imm_memtable.items())
                write_result = write_to_sstable_file(self._path, index, sorted_store)
                bf = bloom_filter.write_bloom_filter(self._path, index, sorted_store, config.BLOOM_FALSE_POSITIVE_RATE)

                with self._lock.write():
                    self._sparse_indexes[index] = write_result[0]
                    self._bloom_filters[index] = bf
                    self._bytes_written_disk += os.path.getsize(self._path(f"sst_{index}"))
                    self._manifest.add(0, f"sst_{index}", write_result[1], write_result[2])
                    self._manifest.save()
                    try:
                        os.remove(self._path(wal_file_name))
                    except FileNotFoundError:
                        pass
                    l0_count = sum(1 for entry in self._manifest.entries if entry["level"] == 0)
                    if l0_count >= config.MAX_L0_FILES:
                        self._compact()
                    self._imm_memtable = None
                    self._imm_entries = 0
                    try:
                        self._save_meta()
                    except Exception:
                        pass
            finally:
                # Wake any writers parked on back-pressure.
                with self._flush_drained:
                    self._flush_drained.notify_all()

        self._flush_thread = threading.Thread(target=_threaded_funct)
        self._flush_thread.start()

    def _load_sstables(self):
        """Load SSTable sidecars and recover the highest allocated file index."""
        max_index = 0
        for entry in self._manifest.entries:
            index_counter = sst_index(entry)
            if index_counter > max_index:
                max_index = index_counter

            try:
                self._bloom_filters[index_counter] = bloom_filter.load_bloom_filter(self._path, index_counter)
            except FileNotFoundError:
                raise RuntimeError(f"Bloom filter file missing for SST {index_counter} referenced by manifest")

            try:
                self._sparse_indexes[index_counter] = load_sparse_index(self._path, index_counter)
            except FileNotFoundError:
                raise RuntimeError(f"Sparse index file missing for SST {index_counter} referenced by manifest")

        self._index_counter = max_index

    def _set_key_seq_value(self, key: str, value, seq: int):
        """Append one already-sequenced value to the active memtable."""
        if key not in self._store:
            self._store[key] = []
        self._store[key].append((seq, value))
        val_size = _TOMBSTONE_BYTES if value is _TOMBSTONE else len(value)
        self._entries += len(key) + val_size

    def _set(self, key: str, value: str, seq: int):
        """Apply a set mutation after it has been assigned a sequence number."""
        self._set_key_seq_value(key, value, seq)
        self._bytes_written_user += len(key) + len(value)

        if self._entries >= config.MAX_MEMTABLE_SIZE:
            self._flush()

    def _delete(self, key: str, seq: int):
        """Apply a delete mutation by adding a tombstone to the memtable."""
        self._set_key_seq_value(key, _TOMBSTONE, seq)
        self._bytes_written_user += len(key)

        if self._entries >= config.MAX_MEMTABLE_SIZE:
            self._flush()

    def _wait_for_flush_to_drain(self):
        """Wait for the in-flight flush before allowing more buffered writes."""
        with self._flush_drained:
            while self._imm_memtable is not None:
                self._flush_drained.wait()

    def get(self, key: str, at=None):
        """Return ``key``'s value, or ``None`` when it is missing or deleted.

        Args:
            key: Key to look up.
            at: Optional snapshot sequence returned by :meth:`snapshot`.
        """
        with self._lock.read():
            if key in self._store:
                raw_value = get_raw_value_from_table_at(self._store, key, at)
                if raw_value is not None:
                    return None if raw_value is _TOMBSTONE else raw_value

            if self._imm_memtable is not None and key in self._imm_memtable:
                raw_value = get_raw_value_from_table_at(self._imm_memtable, key, at)
                if raw_value is not None:
                    return None if raw_value is _TOMBSTONE else raw_value

            sorted_entries = sorted(
                self._manifest.entries,
                key=lambda e: (0, -sst_index(e)) if e["level"] == 0 else (e["level"], 0),
            )

            for entry in sorted_entries:
                if entry["level"] > 0 and (key > entry["max_key"] or key < entry["min_key"]):
                    continue

                index = sst_index(entry)
                if not self._bloom_filters[index].contains(key):
                    continue

                sparse_idx = self._sparse_indexes[index]
                if sparse_idx:
                    result = search_sstable_with_index(self._path, index, sparse_idx, key, at)
                else:
                    tuples = read_sstable_tuples(self._path, index)
                    result = binary_search(tuples, key, at)

                if result is _TOMBSTONE:
                    return None
                if result is not None:
                    return result

            return None

    def scan(self, start: str, end: str, at=None):
        """Return visible ``(key, value)`` pairs in the inclusive key range."""
        return self._materialize_range(start, end, at)

    def iter(self, start: str, end: str, at=None):
        """Return an iterator over a materialized, consistent range result.

        The range is materialized before this method returns, so callers may
        safely write to the store while consuming the iterator.
        """
        return iter(self._materialize_range(start, end, at))

    def _materialize_range(self, start: str, end: str, at=None):
        """Merge memtables and SSTables into one sorted, visible key range."""
        results = []
        with self._lock.read():
            sources = []
            for entry in sorted(self._manifest.entries, key=lambda e: (e["level"], -sst_index(e))):
                index = sst_index(entry)
                sources.append(iter_sstable_from(self._path, index, self._sparse_indexes[index], start))
            if self._imm_memtable is not None:
                sources.append(memtable_iter(self._imm_memtable))
            sources.append(memtable_iter(self._store))

            seen_key = None
            best_seq = -1
            best_value = None

            def should_yield(val, seq):
                """Return whether this version is visible in the requested range."""
                return val is not _TOMBSTONE and (at is None or seq <= at)

            for key, seq, value in heapq.merge(*sources):
                if key > end:
                    break
                if key < start:
                    continue
                if key != seen_key:
                    if seen_key is not None and should_yield(best_value, best_seq):
                        results.append((seen_key, best_value))
                    seen_key = key
                    best_seq = seq
                    best_value = value
                elif seq > best_seq and (at is None or seq <= at):
                    best_seq = seq
                    best_value = value

            if seen_key is not None and should_yield(best_value, best_seq):
                results.append((seen_key, best_value))

        return results

    def stats(self):
        """Return storage layout and write-accounting statistics."""
        with self._lock.read():
            mp = {}

            for entry in self._manifest.entries:
                mp[entry["level"]] = mp.get(entry["level"], 0) + 1

            sstable_count = len(self._manifest.entries)
            total_disk_size = 0
            sst_file_names = [f for f in glob.glob(self._path("sst_*")) if "." not in os.path.basename(f)]

            for file_name in sst_file_names:
                total_disk_size += os.path.getsize(file_name)

            memtable_size = self._entries + self._imm_entries

            def newest_memtable_value(key):
                """Return key's newest memtable value, preferring the active memtable."""
                if key in self._store:
                    return self._store[key][-1][1]
                if self._imm_memtable:
                    return self._imm_memtable[key][-1][1]

            live_keys = set(self._store)
            if self._imm_memtable:
                live_keys |= set(self._imm_memtable)
            keys_num = sum(1 for key in live_keys if newest_memtable_value(key) is not _TOMBSTONE)
            write_amplification = self._bytes_written_disk / self._bytes_written_user if self._bytes_written_user != 0 else 0

            return {
                "sstable_count": sstable_count,
                "sstables_per_level": mp,
                "total_size_bytes": total_disk_size,
                "memtable_size_bytes": memtable_size,
                "memtable_keys": keys_num,
                "bytes_written_disk": self._bytes_written_disk,
                "bytes_written_user": self._bytes_written_user,
                "write_amplification": write_amplification,
            }

    def dump(self):
        """Return the current visible database contents as a dictionary."""
        return dict(self.scan("", "\U0010FFFF"))

    def set(self, key: str, value: str):
        """Durably queue ``value`` as the newest value for ``key``."""
        while True:
            with self._lock.write():
                if self._imm_memtable is not None and self._entries >= 2 * config.MAX_MEMTABLE_SIZE:
                    pass  # fall through to wait
                else:
                    self._seq += 1
                    seq = self._seq
                    self._wal_write_record("SET", seq, key, value)
                    self._set(key, value, seq)
                    return
            self._wait_for_flush_to_drain()

    def delete(self, key: str):
        """Durably mark ``key`` deleted with a tombstone."""
        while True:
            with self._lock.write():
                if self._imm_memtable is not None and self._entries >= 2 * config.MAX_MEMTABLE_SIZE:
                    pass
                else:
                    self._seq += 1
                    seq = self._seq
                    self._wal_write_record("DELETE", seq, key)
                    self._delete(key, seq)
                    return
            self._wait_for_flush_to_drain()

    def close(self):
        """Finish any flush, synchronize the WAL, and release the directory."""
        if self._wal.closed:
            return

        if self._flush_thread is not None:
            self._flush_thread.join()

        self._wal.flush()
        os.fsync(self._wal.fileno())
        self._wal.close()

        # Persist final counters so a clean shutdown's stats survive.
        try:
            self._save_meta()
        except Exception:
            pass

        self._release_directory_lock()
