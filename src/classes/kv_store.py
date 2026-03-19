import glob
import os 
import src.config as config 
import src.classes.bloom_filter as bloom_filter 
import src.classes.manifest as manifest 

class KVStore:
    # Static Methods
    @staticmethod 
    def _sst_index(entry):
        return int(entry["file_name"].split("_")[1])

    def __init__(self):
        self._store = {}
        self.entries = 0
        self.index_counter = 0
        self._wal_buffer_count = 0
        self.bloom_filters = {}
        self.sparse_indexes = {}
        self.manifest = manifest.Manifest.load() 
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

    def _replay_line(self, line):
        line = line.strip()
        sp = line.split(" ")
        if sp[0] == "SET":
            self._store[sp[1]] = sp[2]
        elif sp[0] == "DELETE":
            self._store[sp[1]] = config.TOMBSTONE_VALUE

    def _write_sstable(self, index, data):
        write_result = self._write_to_sstable_file(index, data)
        self.sparse_indexes[index] = write_result[0]
        self._write_bloom_filter(data, index)

        return write_result 

    def _write_to_sstable_file(self, index, sorted_store):
        sparse = []
        min_key, max_key = None, None 

        with open(f"sst_{index}", 'w') as file: 
            i = 0

            for key, value in sorted_store:
                if min_key is None: 
                    min_key = key  
                max_key = key 
                i += 1
                offset = file.tell()
                file.write(f"{key} {value}\n")
                
                if i % config.SPARSE_INDEX_N == 0:
                    sparse.append((key, offset))

        with open(f"sst_{index}.index", 'w') as file: 
            for key, offset in sparse: 
                file.write(f"{key} {offset}\n")

        return (sparse, min_key, max_key) 

    def _write_bloom_filter(self, items, index):
        filter = bloom_filter.BloomFilter(config.BLOOM_FILTER_SIZE)

        for key, _ in items:
            filter.add(key)

        with open(f"sst_{index}.bloom", 'w') as file:
            file.write(filter.serialize())

        self.bloom_filters[index] = filter 

    def _update_manifest(self, level, file_name, min_key, max_key):
        self.manifest.add(level, file_name, min_key, max_key)
        self.manifest.save()

    def _compact_level(self, level):
        entries = [entry for entry in self.manifest.entries if entry["level"] == level]

        if not entries:
            return

        overall_min = min(entry["min_key"] for entry in entries)
        overall_max = max(entry["max_key"] for entry in entries)
        next_entries = [entry for entry in self.manifest.entries if entry["level"] == level + 1 and entry["min_key"] <= overall_max and entry["max_key"] >= overall_min]
        merged = {}

        def read_from_entries_list(entries_list):
            for entry in entries_list:
                index = KVStore._sst_index(entry)

                for key, value in self._build_sstable_tuples(index):
                    merged[key] = value

        read_from_entries_list(next_entries) 
        read_from_entries_list(entries)
        merged = sorted(merged.items())

        for entry in entries + next_entries: 
            # Remove all files used by the index 
            index = KVStore._sst_index(entry)
            os.remove(f"sst_{index}")
            os.remove(f"sst_{index}.bloom")
            os.remove(f"sst_{index}.index")
            self.manifest.remove(entry["file_name"])
            self.bloom_filters.pop(index, None)
            self.sparse_indexes.pop(index, None)

        sstable_file_size = config.MAX_L0_FILES * (10 ** (level + 1))

        for i in range(0, len(merged), sstable_file_size):
            chunk = merged[i:i + sstable_file_size]
            self.index_counter += 1
            self._write_sstable(self.index_counter, chunk)
            self._update_manifest(level + 1, f"sst_{self.index_counter}", chunk[0][0], chunk[-1][0])

    def _compact(self):
        level = 0

        while True:
            self._compact_level(level)
            next_count = sum(1 for entry in self.manifest.entries if entry["level"] == level + 1)
            level_limit = config.MAX_L0_FILES * (10 ** (level + 1))

            if next_count < level_limit: 
                break
            
            level += 1

    def _flush(self):
        self.index_counter += 1
        sorted_store = sorted(self._store.items())
        write_result = self._write_sstable(self.index_counter, sorted_store)
        self._update_manifest(0, f"sst_{self.index_counter}", write_result[1], write_result[2])
        self._store = {}
        self.entries = 0 
        self._wal.flush()
        self._wal.close()
        self._wal = open(config.LOG_FILE_NAME, 'w')
        self._wal.close()
        self._wal = open(config.LOG_FILE_NAME, 'a')
        l0_count = sum(1 for entry in self.manifest.entries if entry["level"] == 0)
        
        if l0_count >= config.MAX_L0_FILES:
            self._compact()

    def _binary_search(self, tuples, key):
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
    
    def _build_sstable_tuples(self, index, index_file=False):
        tuples = []
        file_name = f"sst_{index}.index" if index_file else f"sst_{index}"

        with open(file_name, 'r') as file:
            for line in file: 
                line = line.strip() 
                inner_key, value = line.split(" ")
                # The index files need the int_offset instead of just the string value. 
                value = int(value) if index_file else value 
                tuples.append((inner_key, value))

        return tuples

    def _search_sstables(self, key):
        sorted_entries = sorted(self.manifest.entries, 
            key=lambda entry: (0, -(KVStore._sst_index(entry))) if entry["level"] == 0 else (entry["level"], 0))

        for entry in sorted_entries:
            if entry["level"] > 0 and (key > entry["max_key"] or key < entry["min_key"]):
                continue 

            index = KVStore._sst_index(entry)

            if not self.bloom_filters[index].contains(key):
                continue 
                
            if self.sparse_indexes[index]: 
                sparse_index_result = self._search_sstable_with_index(index, key)

                if sparse_index_result == config.TOMBSTONE_VALUE:
                    return None 

                if sparse_index_result is not None: 
                    return sparse_index_result 
                
                continue
            else:
                tuples = self._build_sstable_tuples(index)
                search_result = self._binary_search(tuples, key)

                if search_result == config.TOMBSTONE_VALUE:
                    return None 

                if search_result is not None:
                    return search_result
        
        return None 
    
    def _search_sstable_with_index(self, index, key):
        if not self.sparse_indexes[index]:
            print(f"No sparse index exists in the sparse_indexes dictionary for {index}!")
            return

        low = 0
        high = len(self.sparse_indexes[index]) - 1
        found = False

        while low <= high:
            mid = (low + high) // 2

            if self.sparse_indexes[index][mid][0] <= key: 
                low = mid + 1 
                found = True
            else:
                high = mid - 1
        
        offset = self.sparse_indexes[index][low - 1][1] if found else 0

        with open(f"sst_{index}", 'r') as file: 
            file.seek(offset)

            for line in file: 
                line = line.strip() 
                inner_key, value = line.split(" ")
                
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
                    key, value = line.split(" ")
                    self._store[key] = value 
            
            try:
                with open(f"sst_{index_counter}.bloom", 'r') as file: 
                    line = file.read() 
                    self.bloom_filters[index_counter] = bloom_filter.BloomFilter.deserialize(line)
            except FileNotFoundError:
                print(f"Bloom filter file does not exist for index {index_counter}!")

            try:
                tuples = self._build_sstable_tuples(index_counter, True)
                self.sparse_indexes[index_counter] = tuples 
            except FileNotFoundError:
                print(f"Index file does not exist for index {index_counter}!")

        self.index_counter = index_counter
        self.entries = sum(len(k) + len(v) for k, v in self._store.items() if v != config.TOMBSTONE_VALUE and v is not None)

    def _set(self, key: str, value: str):
        prev_value = self._store.get(key)
        self._store[key] = value

        if prev_value is None or prev_value == config.TOMBSTONE_VALUE:
            self.entries += len(key) + len(value)
        else:
            # Overwriting a real value, so adjust by the difference in value length. 
            self.entries += (len(value) - len(prev_value))

        if self.entries < config.MAX_MEMTABLE_SIZE:
            return

        # Do the flush. 
        self._flush()

    def _delete(self, key: str):
        prev_value = self._store.get(key)
        self._store[key] = config.TOMBSTONE_VALUE

        # I only want to subtract the entries count when it was a valid value to begin with.
        if prev_value is not None and prev_value != config.TOMBSTONE_VALUE:
            self.entries -= (len(key) + len(prev_value))

    # Public Methods 
    def set(self, key: str, value: str):
        self._wal.write(f"SET {key} {value}\n")
        self._increment_wall_buffer_count()
        self._set(key, value)

    def get(self, key: str):
        raw_value = None

        if key in self._store:
            raw_value = self._store.get(key)
        else:
            raw_value = self._search_sstables(key)
        
        return None if raw_value == config.TOMBSTONE_VALUE else raw_value

    def delete(self, key: str):
        self._wal.write(f"DELETE {key}\n")
        self._increment_wall_buffer_count()
        self._delete(key)

    def close(self):
        self._wal.flush()
        self._wal.close()

    def scan(self, start: str, end: str):
        entries = {}

        for entry in self.manifest.entries:
            index = KVStore._sst_index(entry)
            tuples = self._build_sstable_tuples(index)

            for key, value in tuples:
                if key >= start and key <= end:
                    if value == config.TOMBSTONE_VALUE:
                        entries.pop(key, None)
                    else:
                        entries[key] = value

        for key, value in self._store.items():
            if key >= start and key <= end:
                if value == config.TOMBSTONE_VALUE:
                    entries.pop(key, None)
                else:
                    entries[key] = value

        return [(key, entries[key]) for key in sorted(entries)]