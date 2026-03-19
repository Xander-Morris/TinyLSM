import glob
import os 
import src.config as config 
import src.classes.bloom_filter as bloom_filter 
import src.classes.manifest as manifest 
import src.classes.read_write_lock as read_write_lock
import binascii

class KVStore:
    # Static Methods
    @staticmethod
    def _sst_index(entry):
        return int(entry["file_name"].split("_")[1])

    @staticmethod
    def _parse_sstable_line(line):
        key, value, stored_checksum = line.split(" ")
        computed_checksum = str(binascii.crc32(f"{key} {value}".encode()))

        if stored_checksum != computed_checksum:
            raise ValueError(f"Checksum mismatch for key '{key}': expected {computed_checksum}, got {stored_checksum}")

        return key, value

    @staticmethod
    def _write_to_sstable_file(index, sorted_store):
        sparse = []
        min_key, max_key = None, None

        with open(f"sst_{index}", 'w') as file:
            i = 0
            key_count = 0

            for key, versions in sorted_store:
                key_count += 1
                first_version = True

                for seq, value in versions:
                    if min_key is None:
                        min_key = key
                    max_key = key
                    i += 1
                    offset = file.tell()
                    line = f"{key} {seq} {value}"
                    checksum = binascii.crc32(line.encode())
                    file.write(f"{line} {checksum}\n")

                    if first_version and key_count % config.SPARSE_INDEX_N == 0:
                        sparse.append((key, offset))
                        first_version = False 

        with open(f"sst_{index}.index", 'w') as file:
            for key, offset in sparse:
                file.write(f"{key} {offset}\n")

        return (sparse, min_key, max_key)

    @staticmethod
    def _binary_search(tuples, key):
        low = 0
        high = len(tuples) - 1

        while low <= high:
            mid = (low + high) // 2
            key_at_mid = tuples[mid][0]

            if key == key_at_mid:
                return tuples[mid][1] # return value
            elif key < key_at_mid:
                high = mid - 1
            else:
                low = mid + 1

        return None

    @staticmethod
    def _build_sstable_tuples(index, index_file=False):
        tuples = []
        file_name = f"sst_{index}.index" if index_file else f"sst_{index}"

        with open(file_name, 'r') as file:
            for line in file:
                line = line.strip()
                if index_file:
                    inner_key, value = line.split(" ")
                else:
                    inner_key, value = KVStore._parse_sstable_line(line)
                # The index files need the int_offset instead of just the string value.
                value = int(value) if index_file else value
                tuples.append((inner_key, value))

        return tuples

    # Object-Specific Methods 
    def __init__(self):
        self._store = {}
        self._entries = 0
        self._index_counter = 0
        self._wal_buffer_count = 0
        self._seq = 0 
        self._bloom_filters = {}
        self._sparse_indexes = {}
        self._manifest = manifest.Manifest.load() 
        self._lock = read_write_lock.ReadWriteLock()
        self._load_sstables()

        try:
            with open(config.LOG_FILE_NAME, 'r') as file:
                for line in file: 
                    self._replay_line(line)
        except FileNotFoundError:
            pass

        self._wal = open(config.LOG_FILE_NAME, 'a')

    # Private Methods
    def _increment_wall_buffer_count(self):
        self._wal_buffer_count += 1

        if self._wal_buffer_count >= config.WAL_BUFFER_SIZE:
            self._wal.flush()
            self._wal_buffer_count = 0

    def _restore_key_seq_value(self, key: str, seq: int, value: str):
        if key not in self._store:
            self._store[key] = []
        self._store[key].append((seq, value))
        self._seq = max(self._seq, seq)

    def _replay_line(self, line):
        line = line.strip()
        sp = line.split(" ")
        if sp[0] == "SET":
            self._set_key_seq_value(sp[1], sp[2])
        elif sp[0] == "DELETE":
            self._set_key_seq_value(sp[1], config.TOMBSTONE_VALUE)

    def _write_sstable(self, index, data):
        write_result = KVStore._write_to_sstable_file(index, data)
        self._sparse_indexes[index] = write_result[0]
        self._write_bloom_filter(data, index)

        return write_result 

    def _write_bloom_filter(self, items, index):
        filter = bloom_filter.BloomFilter(config.BLOOM_FILTER_SIZE)

        for key, _ in items:
            filter.add(key)

        with open(f"sst_{index}.bloom", 'w') as file:
            file.write(filter.serialize())

        self._bloom_filters[index] = filter 

    def _update_manifest(self, level, file_name, min_key, max_key):
        self._manifest.add(level, file_name, min_key, max_key)
        self._manifest.save()

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

                for key, seq, value in KVStore._build_sstable_tuples(index):
                    if key not in merged:
                        merged[key] = []
                    merged[key].append((seq, value))

        read_from_entries_list(next_entries) 
        read_from_entries_list(entries)
        merged = sorted(merged.items())

        for entry in entries + next_entries: 
            # Remove all files used by the index 
            index = KVStore._sst_index(entry)
            os.remove(f"sst_{index}")
            os.remove(f"sst_{index}.bloom")
            os.remove(f"sst_{index}.index")
            self._manifest.remove(entry["file_name"])
            self._bloom_filters.pop(index, None)
            self._sparse_indexes.pop(index, None)

        sstable_file_size = config.MAX_L0_FILES * (10 ** (level + 1))

        for i in range(0, len(merged), sstable_file_size):
            chunk = merged[i:i + sstable_file_size]
            self._index_counter += 1
            self._write_sstable(self._index_counter, chunk)
            self._update_manifest(level + 1, f"sst_{self._index_counter}", chunk[0][0], chunk[-1][0])

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
        self._index_counter += 1
        sorted_store = sorted(self._store.items())
        write_result = self._write_sstable(self._index_counter, sorted_store)
        self._update_manifest(0, f"sst_{self._index_counter}", write_result[1], write_result[2])
        self._store = {}
        self._entries = 0 
        self._wal.flush()
        self._wal.close()
        self._wal = open(config.LOG_FILE_NAME, 'w')
        self._wal.close()
        self._wal = open(config.LOG_FILE_NAME, 'a')
        l0_count = sum(1 for entry in self._manifest.entries if entry["level"] == 0)
        
        if l0_count >= config.MAX_L0_FILES:
            self._compact()

    def _search_sstables(self, key):
        sorted_entries = sorted(self._manifest.entries, 
            key=lambda entry: (0, -(KVStore._sst_index(entry))) if entry["level"] == 0 else (entry["level"], 0))

        for entry in sorted_entries:
            if entry["level"] > 0 and (key > entry["max_key"] or key < entry["min_key"]):
                continue 

            index = KVStore._sst_index(entry)

            if not self._bloom_filters[index].contains(key):
                continue 
                
            if self._sparse_indexes[index]: 
                sparse_index_result = self._search_sstable_with_index(index, key)

                if sparse_index_result == config.TOMBSTONE_VALUE:
                    return None 

                if sparse_index_result is not None: 
                    return sparse_index_result 
                
                continue
            else:
                tuples = KVStore._build_sstable_tuples(index)
                search_result = KVStore._binary_search(tuples, key)

                if search_result == config.TOMBSTONE_VALUE:
                    return None 

                if search_result is not None:
                    return search_result
        
        return None 
    
    def _search_sstable_with_index(self, index, key):
        if not self._sparse_indexes[index]:
            print(f"No sparse index exists in the sparse_indexes dictionary for {index}!")
            return

        low = 0
        high = len(self._sparse_indexes[index]) - 1
        found = False

        while low <= high:
            mid = (low + high) // 2

            if self._sparse_indexes[index][mid][0] <= key: 
                low = mid + 1 
                found = True
            else:
                high = mid - 1
        
        offset = self._sparse_indexes[index][low - 1][1] if found else 0

        with open(f"sst_{index}", 'r') as file: 
            file.seek(offset)

            for line in file: 
                line = line.strip()
                inner_key, value = KVStore._parse_sstable_line(line)

                if key == inner_key:
                    return value 
                elif key < inner_key:
                    break 

        return None 

    def _load_sstables(self):
        sst_file_names = [f for f in glob.glob("sst_*") if "." not in f]
        sorted_file_names = sorted(sst_file_names, key=lambda f: int(f.split("_")[1])) # gets the index counter, like in sst_3, we get 3 and sort by that index with respect to the other files
        index_counter = 0

        for file_name in sorted_file_names:
            index_counter = int(file_name.split("_")[1])

            with open(file_name, 'r') as file:
                for line in file: 
                    line = line.strip()
                    key, seq, value = KVStore._parse_sstable_line(line)
                    self._restore_key_seq_value(key, seq, value)
            
            try:
                with open(f"sst_{index_counter}.bloom", 'r') as file: 
                    line = file.read() 
                    self._bloom_filters[index_counter] = bloom_filter.BloomFilter.deserialize(line)
            except FileNotFoundError:
                print(f"Bloom filter file does not exist for index {index_counter}!")

            try:
                tuples = KVStore._build_sstable_tuples(index_counter, True)
                self._sparse_indexes[index_counter] = tuples 
            except FileNotFoundError:
                print(f"Index file does not exist for index {index_counter}!")

        self._index_counter = index_counter
        self._entries = sum(
            len(k) + len(versions[-1][1])
            for k, versions in self._store.items()
            if versions and versions[-1][1] != config.TOMBSTONE_VALUE
        )

    def _set_key_seq_value(self, key: str, value: str):
        self._seq += 1
        if key not in self._store:
            self._store[key] = []
        self._store[key].append((self._seq, value))

    def _get_prev_value(self, key: str):
        versions = self._store.get(key)

        return versions[-1][1] if versions else None

    def _set(self, key: str, value: str):
        prev_value = self._get_prev_value(key)
        self._set_key_seq_value(key, value)

        if prev_value is None or prev_value == config.TOMBSTONE_VALUE:
            self._entries += len(key) + len(value)
        else:
            # Overwriting a real value, so adjust by the difference in value length. 
            self._entries += (len(value) - len(prev_value))

        if self._entries < config.MAX_MEMTABLE_SIZE:
            return

        # Do the flush. 
        self._flush()

    def _delete(self, key: str):
        prev_value = self._get_prev_value(key)
        self._set_key_seq_value(key, config.TOMBSTONE_VALUE)

        # I only want to subtract the entries count when it was a valid value to begin with.
        if prev_value is not None and prev_value != config.TOMBSTONE_VALUE:
            self._entries -= (len(key) + len(prev_value))

    # Public Methods 
    # Read Operations
    def get(self, key: str, at=None):
        with self._lock.read():
            raw_value = None

            if key in self._store:
                versions = self._store.get(key)
                raw_value = versions[-1][1]
                
                if at is not None: 
                    raw_value = None 

                    for i in range(len(versions) - 1, -1, -1):
                        if versions[i][0] <= at:
                            raw_value = versions[i][1]
                            break
            else:
                raw_value = self._search_sstables(key)
            
            return None if raw_value == config.TOMBSTONE_VALUE else raw_value

    def scan(self, start: str, end: str):
        with self._lock.read():
            entries = {}

            for entry in self._manifest.entries:
                index = KVStore._sst_index(entry)
                tuples = KVStore._build_sstable_tuples(index)

                for key, value in tuples:
                    if key >= start and key <= end:
                        if value == config.TOMBSTONE_VALUE:
                            entries.pop(key, None)
                        else:
                            entries[key] = value

            for key, versions in self._store.items():
                value = versions[-1][1] # Use the latest value. 

                if key >= start and key <= end:
                    if value == config.TOMBSTONE_VALUE:
                        entries.pop(key, None)
                    else:
                        entries[key] = value

            return [(key, entries[key]) for key in sorted(entries)]
        
    def stats(self):
        with self._lock.read():
            # SSTable count per level is calculated first.
            mp = {}

            for entry in self._manifest.entries: 
                mp[entry["level"]] = mp.get(entry["level"], 0) + 1
            
            # The total SSTable count is next, which is just the length of the manifest entries list.
            sstable_count = len(self._manifest.entries)

            # Total disk size is next.
            total_disk_size = 0
            sst_file_names = [f for f in glob.glob("sst_*") if "." not in f]

            for file_name in sst_file_names:
                total_disk_size += os.path.getsize(file_name)

            # The memtable size is next, which is just the entries variable.
            memtable_size = self._entries 

            # Number of *live* keys (not tombstones) in the memtable is next. 
            keys_num = sum([1 for key, value in self._store.items() if value != config.TOMBSTONE_VALUE and value is not None])

            return {
                "sstable_count": sstable_count, 
                "sstables_per_level": mp, 
                "total_size_bytes": total_disk_size,
                "memtable_size_bytes": memtable_size,
                "memtable_keys": keys_num,
            }
        
    # Write Operations
    def set(self, key: str, value: str):
        with self._lock.write():
            self._wal.write(f"SET {key} {value}\n")
            self._increment_wall_buffer_count()
            self._set(key, value)

    def delete(self, key: str):
        with self._lock.write():
            self._wal.write(f"DELETE {key}\n")
            self._increment_wall_buffer_count()
            self._delete(key)

    # Close
    def close(self):
        if self._wal.closed:
            return

        self._wal.flush()
        self._wal.close()