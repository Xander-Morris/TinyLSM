import glob
import os
import json
import src.config as config
import src.classes.bloom_filter as bloom_filter
import src.classes.manifest as manifest
import src.classes.read_write_lock as read_write_lock
import binascii
import threading
import heapq


# Identity-checked sentinel — distinct from any user value.
class _TombstoneType:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "<TOMBSTONE>"


_TOMBSTONE = _TombstoneType()
_TOMBSTONE_BYTES = 1  # accounting weight for tombstone marker in memtable


class KVStore:
    # Static Methods (pure helpers — no file I/O)
    @staticmethod
    def _sst_index(entry):
        return int(entry["file_name"].split("_")[1])

    @staticmethod
    def _parse_sstable_line(line):
        line = line.rstrip("\r\n")
        if "\t" not in line:
            raise ValueError(f"Malformed SSTable line: {line!r}")
        payload, _, crc_str = line.rpartition("\t")
        try:
            stored_crc = int(crc_str)
        except ValueError:
            raise ValueError(f"Bad CRC field in SSTable line: {line!r}")
        computed_crc = binascii.crc32(payload.encode("utf-8"))
        if stored_crc != computed_crc:
            raise ValueError(f"Checksum mismatch: expected {computed_crc}, got {stored_crc}")
        record = json.loads(payload)
        value = _TOMBSTONE if record.get("t") else record["v"]
        return record["k"], int(record["s"]), value

    @staticmethod
    def _pick_version(versions, at=None):
        if at is None:
            return versions[-1][1]
        for i in range(len(versions) - 1, -1, -1):
            if versions[i][0] <= at:
                return versions[i][1]
        return None

    @staticmethod
    def _binary_search(tuples, key, at=None):
        low = 0
        high = len(tuples) - 1

        while low <= high:
            mid = (low + high) // 2
            key_at_mid = tuples[mid][0]

            if key == key_at_mid:
                versions = [(seq, value) for k, seq, value in tuples if k == key]
                return KVStore._pick_version(versions, at)
            elif key < key_at_mid:
                high = mid - 1
            else:
                low = mid + 1

        return None

    @staticmethod
    def _get_raw_value_from_table_at(entries, key: str, at=None):
        versions = entries.get(key)
        return KVStore._pick_version(versions, at)

    @staticmethod
    def _search_sparse_index_for_key_offset(sparse_index, key):
        low = 0
        high = len(sparse_index) - 1
        found = False

        while low <= high:
            mid = (low + high) // 2

            if sparse_index[mid][0] <= key:
                low = mid + 1
                found = True
            else:
                high = mid - 1

        offset = sparse_index[low - 1][1] if found else 0
        return offset

    @staticmethod
    def _memtable_iter(table):
        for key, versions in sorted(table.items()):
            for seq, value in versions:
                yield (key, seq, value)

    @staticmethod
    def _parse_wal_record(line):
        line = line.rstrip("\r\n")
        if "\t" not in line:
            return None
        payload, _, crc_str = line.rpartition("\t")
        try:
            crc = int(crc_str)
        except ValueError:
            return None
        if binascii.crc32(payload.encode("utf-8")) != crc:
            return None
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return None

    # Object-Specific Methods
    def __init__(self, data_dir=None):
        self._data_dir = os.path.abspath(data_dir) if data_dir else os.path.abspath(os.getcwd())
        os.makedirs(self._data_dir, exist_ok=True)
        self._store = {}
        self._imm_memtable = None
        self._imm_entries = 0
        self._entries = 0
        self._index_counter = 0
        self._wal_buffer_count = 0
        self._bytes_written_disk = 0
        self._bytes_written_user = 0
        self._seq = 0
        self._bloom_filters = {}
        self._sparse_indexes = {}
        self._flush_thread = None
        self._manifest = manifest.Manifest.load(self._data_dir)
        self._lock = read_write_lock.ReadWriteLock()
        self._cleanup_orphan_sst_files()
        self._load_sstables()

        try:
            with open(self._path(config.LOG_FILE_NAME + ".flushing"), 'r', encoding='utf-8') as file:
                for line in file:
                    if not self._replay_line(line):
                        break
        except FileNotFoundError:
            pass

        try:
            with open(self._path(config.LOG_FILE_NAME), 'r', encoding='utf-8') as file:
                for line in file:
                    if not self._replay_line(line):
                        break
        except FileNotFoundError:
            pass

        self._wal = open(self._path(config.LOG_FILE_NAME), 'a', encoding='utf-8')

    def _path(self, name):
        return os.path.join(self._data_dir, name)

    def _cleanup_orphan_sst_files(self):
        """Delete SST data/bloom/index files not referenced by the manifest.
        Crash-mid-compaction can leave either side orphaned; the manifest is truth."""
        expected = {KVStore._sst_index(entry) for entry in self._manifest.entries}
        on_disk = set()
        for path in glob.glob(self._path("sst_*")):
            base = os.path.basename(path)
            stem = base.split(".", 1)[0]
            try:
                on_disk.add(int(stem.split("_", 1)[1]))
            except (ValueError, IndexError):
                continue

        for idx in on_disk - expected:
            for ext in ("", ".bloom", ".index"):
                try:
                    os.remove(self._path(f"sst_{idx}{ext}"))
                except FileNotFoundError:
                    pass

    # Instance file-I/O helpers
    def _write_to_sstable_file(self, index, sorted_store):
        sparse = []
        min_key, max_key = None, None

        with open(self._path(f"sst_{index}"), 'w', encoding='utf-8') as file:
            key_count = 0

            for key, versions in sorted_store:
                key_count += 1
                first_version = True

                for seq, value in versions:
                    if min_key is None:
                        min_key = key
                    max_key = key
                    offset = file.tell()
                    record = {"k": key, "s": seq}
                    if value is _TOMBSTONE:
                        record["t"] = True
                    else:
                        record["v"] = value
                    payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
                    checksum = binascii.crc32(payload.encode("utf-8"))
                    file.write(f"{payload}\t{checksum}\n")

                    if first_version and key_count % config.SPARSE_INDEX_N == 0:
                        sparse.append((key, offset))
                        first_version = False

            file.flush()
            os.fsync(file.fileno())

        with open(self._path(f"sst_{index}.index"), 'w', encoding='utf-8') as file:
            for key, offset in sparse:
                record = {"k": key, "o": offset}
                file.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            file.flush()
            os.fsync(file.fileno())

        return (sparse, min_key, max_key)

    def _build_sstable_tuples(self, index, index_file=False):
        tuples = []
        file_name = self._path(f"sst_{index}.index") if index_file else self._path(f"sst_{index}")

        with open(file_name, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.rstrip("\r\n")
                if not line:
                    continue
                if index_file:
                    record = json.loads(line)
                    tuples.append((record["k"], int(record["o"])))
                else:
                    inner_key, seq, value = KVStore._parse_sstable_line(line)
                    tuples.append((inner_key, seq, value))

        return tuples

    def _sstable_iter_from(self, index, sparse_index, start_key):
        offset = KVStore._search_sparse_index_for_key_offset(sparse_index, start_key)

        with open(self._path(f"sst_{index}"), 'r', encoding='utf-8') as file:
            file.seek(offset)

            for line in file:
                key, seq, value = KVStore._parse_sstable_line(line)

                if key < start_key:
                    continue
                yield (key, seq, value)

    def _search_sstable_with_index(self, index, sparse_index, key, at=None):
        offset = KVStore._search_sparse_index_for_key_offset(sparse_index, key)
        versions = []

        with open(self._path(f"sst_{index}"), 'r', encoding='utf-8') as file:
            file.seek(offset)

            for line in file:
                inner_key, seq, value = KVStore._parse_sstable_line(line)

                if key == inner_key:
                    versions.append((seq, value))
                elif key < inner_key:
                    break

        return KVStore._pick_version(versions, at) if versions else None

    # Private Methods
    def _wal_write_record(self, op, seq, key, value=None):
        record = {"op": op, "seq": seq, "key": key}
        if value is not None:
            record["value"] = value
        payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        crc = binascii.crc32(payload.encode("utf-8"))
        self._wal.write(f"{payload}\t{crc}\n")
        self._increment_wal_buffer_count()

    def _increment_wal_buffer_count(self):
        self._wal_buffer_count += 1

        if self._wal_buffer_count >= config.WAL_BUFFER_SIZE:
            self._wal.flush()
            os.fsync(self._wal.fileno())
            self._wal_buffer_count = 0

    def _replay_line(self, line):
        record = KVStore._parse_wal_record(line)
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
        write_result = self._write_to_sstable_file(index, data)
        self._sparse_indexes[index] = write_result[0]
        self._write_bloom_filter(data, index)
        self._bytes_written_disk += os.path.getsize(self._path(f"sst_{index}"))

        return write_result

    def _write_bloom_filter(self, items, index):
        filter = bloom_filter.BloomFilter.for_capacity(len(items), config.BLOOM_FALSE_POSITIVE_RATE)

        for key, _ in items:
            filter.add(key)

        with open(self._path(f"sst_{index}.bloom"), 'w', encoding='utf-8') as file:
            file.write(filter.serialize())
            file.flush()
            os.fsync(file.fileno())

        self._bloom_filters[index] = filter

    def _compact_level(self, level):
        entries = [entry for entry in self._manifest.entries if entry["level"] == level]

        if not entries:
            return

        overall_min = min(entry["min_key"] for entry in entries)
        overall_max = max(entry["max_key"] for entry in entries)
        next_entries = [entry for entry in self._manifest.entries if entry["level"] == level + 1 and entry["min_key"] <= overall_max and entry["max_key"] >= overall_min]
        merged = {}

        def read_from_entries_list(entries_list):
            for entry in entries_list:
                index = KVStore._sst_index(entry)

                for key, seq, value in self._build_sstable_tuples(index):
                    if key not in merged:
                        merged[key] = []
                    merged[key].append((seq, value))

        read_from_entries_list(next_entries)
        read_from_entries_list(entries)

        for key in merged:
            merged[key].sort(key=lambda x: x[0])
            merged[key] = [merged[key][-1]]

        surviving = {}
        for key, versions in merged.items():
            if versions[0][1] is _TOMBSTONE:
                has_older = any(
                    e["level"] > level + 1 and e["min_key"] <= key <= e["max_key"]
                    for e in self._manifest.entries
                )
                if not has_older:
                    continue
            surviving[key] = versions

        merged = sorted(surviving.items())

        # Step 1: write all new SST files (data, bloom, index) durably to disk
        # BEFORE touching the manifest or deleting old files. Crash before
        # manifest update = orphan new files cleaned on next boot.
        sstable_key_count = config.MAX_L0_FILES * (10 ** (level + 1))
        new_entries = []
        for i in range(0, len(merged), sstable_key_count):
            chunk = merged[i:i + sstable_key_count]
            self._index_counter += 1
            new_idx = self._index_counter
            self._write_sstable(new_idx, chunk)
            new_entries.append((level + 1, f"sst_{new_idx}", chunk[0][0], chunk[-1][0]))

        # Step 2: single atomic manifest update — add new and remove old together.
        for lvl, fname, mink, maxk in new_entries:
            self._manifest.add(lvl, fname, mink, maxk)
        for entry in entries + next_entries:
            self._manifest.remove(entry["file_name"])
        self._manifest.save()

        # Step 3: delete old files. Crash here = orphan old files cleaned on boot.
        for entry in entries + next_entries:
            index = KVStore._sst_index(entry)
            for ext in ("", ".bloom", ".index"):
                try:
                    os.remove(self._path(f"sst_{index}{ext}"))
                except FileNotFoundError:
                    pass
            self._bloom_filters.pop(index, None)
            self._sparse_indexes.pop(index, None)

    def _compact(self):
        level = 0

        while True:
            self._compact_level(level)
            next_count = sum(1 for entry in self._manifest.entries if entry["level"] == level + 1)
            level_limit = config.MAX_L0_FILES * (10 ** (level + 1))

            if next_count < level_limit:
                break

            level += 1

    def _flush(self):
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
        os.rename(self._path(config.LOG_FILE_NAME), self._path(config.LOG_FILE_NAME + ".flushing"))
        self._wal = open(self._path(config.LOG_FILE_NAME), 'a', encoding='utf-8')

        def _threaded_funct():
            sorted_store = sorted(self._imm_memtable.items())
            write_result = self._write_to_sstable_file(index, sorted_store)

            bf = bloom_filter.BloomFilter.for_capacity(len(sorted_store), config.BLOOM_FALSE_POSITIVE_RATE)
            for key, _ in sorted_store:
                bf.add(key)
            with open(self._path(f"sst_{index}.bloom"), 'w', encoding='utf-8') as file:
                file.write(bf.serialize())
                file.flush()
                os.fsync(file.fileno())
            with open(self._path("seq.tmp"), 'w', encoding='utf-8') as file:
                file.write(str(self._seq))
                file.flush()
                os.fsync(file.fileno())
            os.replace(self._path("seq.tmp"), self._path("seq"))

            with self._lock.write():
                self._sparse_indexes[index] = write_result[0]
                self._bloom_filters[index] = bf
                self._bytes_written_disk += os.path.getsize(self._path(f"sst_{index}"))
                self._manifest.add(0, f"sst_{index}", write_result[1], write_result[2])
                self._manifest.save()
                try:
                    os.remove(self._path(config.LOG_FILE_NAME + ".flushing"))
                except FileNotFoundError:
                    pass
                l0_count = sum(1 for entry in self._manifest.entries if entry["level"] == 0)
                if l0_count >= config.MAX_L0_FILES:
                    self._compact()
                self._imm_memtable = None
                self._imm_entries = 0

        self._flush_thread = threading.Thread(target=_threaded_funct)
        self._flush_thread.start()

    def _load_sstables(self):
        try:
            with open(self._path("seq"), 'r', encoding='utf-8') as file:
                raw = file.read().strip()
                if raw:
                    self._seq = int(raw)
        except (FileNotFoundError, ValueError):
            pass

        max_index = 0
        for entry in self._manifest.entries:
            index_counter = KVStore._sst_index(entry)
            if index_counter > max_index:
                max_index = index_counter

            try:
                with open(self._path(f"sst_{index_counter}.bloom"), 'r', encoding='utf-8') as file:
                    self._bloom_filters[index_counter] = bloom_filter.BloomFilter.deserialize(file.read())
            except FileNotFoundError:
                raise RuntimeError(f"Bloom filter file missing for SST {index_counter} referenced by manifest")

            try:
                self._sparse_indexes[index_counter] = self._build_sstable_tuples(index_counter, True)
            except FileNotFoundError:
                raise RuntimeError(f"Sparse index file missing for SST {index_counter} referenced by manifest")

        self._index_counter = max_index

    def _set_key_seq_value(self, key: str, value, seq: int):
        if key not in self._store:
            self._store[key] = []
        self._store[key].append((seq, value))
        val_size = _TOMBSTONE_BYTES if value is _TOMBSTONE else len(value)
        self._entries += len(key) + val_size

    def _set(self, key: str, value: str, seq: int):
        self._set_key_seq_value(key, value, seq)
        self._bytes_written_user += len(key) + len(value)

        if self._entries >= config.MAX_MEMTABLE_SIZE:
            self._flush()

    def _delete(self, key: str, seq: int):
        self._set_key_seq_value(key, _TOMBSTONE, seq)
        self._bytes_written_user += len(key)

        if self._entries >= config.MAX_MEMTABLE_SIZE:
            self._flush()

    # Public Methods
    # Read Operations
    def get(self, key: str, at=None):
        with self._lock.read():
            if key in self._store:
                raw_value = KVStore._get_raw_value_from_table_at(self._store, key, at)
                if raw_value is not None:
                    return None if raw_value is _TOMBSTONE else raw_value

            if self._imm_memtable is not None and key in self._imm_memtable:
                raw_value = KVStore._get_raw_value_from_table_at(self._imm_memtable, key, at)
                if raw_value is not None:
                    return None if raw_value is _TOMBSTONE else raw_value

            sorted_entries = sorted(
                self._manifest.entries,
                key=lambda e: (0, -KVStore._sst_index(e)) if e["level"] == 0 else (e["level"], 0),
            )

            for entry in sorted_entries:
                if entry["level"] > 0 and (key > entry["max_key"] or key < entry["min_key"]):
                    continue

                index = KVStore._sst_index(entry)
                if not self._bloom_filters[index].contains(key):
                    continue

                sparse_idx = self._sparse_indexes[index]
                if sparse_idx:
                    result = self._search_sstable_with_index(index, sparse_idx, key, at)
                else:
                    tuples = self._build_sstable_tuples(index)
                    result = KVStore._binary_search(tuples, key, at)

                if result is _TOMBSTONE:
                    return None
                if result is not None:
                    return result

            return None

    def scan(self, start: str, end: str, at=None):
        return list(self.iter(start, end, at))

    def iter(self, start: str, end: str, at=None):
        with self._lock.read():
            sources = []
            for entry in sorted(self._manifest.entries, key=lambda e: (e["level"], -KVStore._sst_index(e))):
                index = KVStore._sst_index(entry)
                sources.append(self._sstable_iter_from(index, self._sparse_indexes[index], start))
            if self._imm_memtable is not None:
                sources.append(KVStore._memtable_iter(self._imm_memtable))
            sources.append(KVStore._memtable_iter(self._store))

            seen_key = None
            best_seq = -1
            best_value = None

            def should_yield(val, seq):
                return val is not _TOMBSTONE and (at is None or seq <= at)

            for key, seq, value in heapq.merge(*sources):
                if key > end:
                    break
                if key < start:
                    continue
                if key != seen_key:
                    if seen_key is not None and should_yield(best_value, best_seq):
                        yield seen_key, best_value
                    seen_key = key
                    best_seq = seq
                    best_value = value
                elif seq > best_seq and (at is None or seq <= at):
                    best_seq = seq
                    best_value = value

            if seen_key is not None and should_yield(best_value, best_seq):
                yield seen_key, best_value

    def stats(self):
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
            keys_num = sum(
                1 for _, versions in self._store.items()
                if versions[-1][1] is not _TOMBSTONE and versions[-1][1] is not None
            )
            write_amplification = self._bytes_written_disk / self._bytes_written_user if self._bytes_written_user != 0 else 0

            return {
                "sstable_count": sstable_count,
                "sstables_per_level": mp,
                "total_size_bytes": total_disk_size,
                "memtable_size_bytes": memtable_size,
                "memtable_keys": keys_num,
                "bytes_written_disk": self._bytes_written_disk,
                "write_amplification": write_amplification,
            }

    def dump(self):
        return dict(self.scan("", "\U0010FFFF"))

    # Write Operations
    def set(self, key: str, value: str):
        with self._lock.write():
            self._seq += 1
            seq = self._seq
            self._wal_write_record("SET", seq, key, value)
            self._set(key, value, seq)

    def delete(self, key: str):
        with self._lock.write():
            self._seq += 1
            seq = self._seq
            self._wal_write_record("DELETE", seq, key)
            self._delete(key, seq)

    # Close
    def close(self):
        if self._wal.closed:
            return

        if self._flush_thread is not None:
            self._flush_thread.join()

        self._wal.flush()
        os.fsync(self._wal.fileno())
        self._wal.close()
