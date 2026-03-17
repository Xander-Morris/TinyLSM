import glob
import os 
import config 
import classes.bloom_filter as bloom_filter 

class KVStore:
    def __init__(self):
        self._store = {}
        self.entries = 0
        self.index_counter = 0
        self.bloom_filters = {}
        self.sparse_indexes = {}
        self._load_sstables()

    # Private Methods
    def _write_to_sstable_file(self, index, sorted_store):
        sparse = []

        with open(f"sst_{index}", 'w') as file: 
            i = 0

            for key, value in sorted_store:
                if value == config.TOMBSTONE_VALUE: 
                    continue 

                i += 1
                offset = file.tell()
                file.write(f"{key} {value}\n")
                
                if i % config.SPARSE_INDEX_N == 0:
                    sparse.append((key, offset))

        with open(f"sst_{index}.index", 'w') as file: 
            for key, offset in sparse: 
                file.write(f"{key} {offset}\n")

    def _write_bloom_filter(self, items, index):
        filter = bloom_filter.BloomFilter(config.BLOOM_FILTER_SIZE)

        for key, value in items:
            if value == config.TOMBSTONE_VALUE:
                continue 

            filter.add(key)

        with open(f"sst_{index}.bloom", 'w') as file:
            file.write(filter.serialize())

        self.bloom_filters[index] = filter 

    def _compact(self):
        sorted_dict = {}

        for index in range(1, self.index_counter + 1):
            to_add = self._build_sstable_tuples(index)
            
            for key, value in to_add:
                sorted_dict[key] = value 
        
        sorted_dict = sorted(sorted_dict.items())

        for index in range(1, self.index_counter + 1):
            os.remove(f"sst_{index}")
            os.remove(f"sst_{index}.bloom")
        
        self._write_to_sstable_file(1, sorted_dict) 
        self.bloom_filters = {}
        self._write_bloom_filter(sorted_dict, 1)
        self.index_counter = 1

    def _flush(self):
        self.index_counter += 1
        sorted_store = sorted(self._store.items())
        self._write_to_sstable_file(self.index_counter, sorted_store)
        self._write_bloom_filter(self._store.items(), self.index_counter)
        self._store = {}
        self.entries = 0 

        with open(config.LOG_FILE_NAME, 'w') as file:
            file.write("")

        if self.index_counter >= config.MAX_SSTABLES:
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
    
    def _build_sstable_tuples(self, index):
        tuples = []

        with open(f"sst_{index}", 'r') as file:
            for line in file: 
                line = line.strip() 
                inner_key, value = line.split(" ")
                tuples.append((inner_key, value))

        return tuples

    def _search_sstables(self, key):
        for index in range(self.index_counter, 0, -1):
            if not self.bloom_filters[index].contains(key):
                continue 

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
        high = len(self.sparse_indexes[index])
        found = False

        while low < high:
            mid = (low + high) // 2

            if self.sparse_indexes[index][mid] <= key: 
                low = mid 
                found = True
            else:
                high = mid - 1
        
        offset = self.sparse_indexes[low][1] if found else 0

        with open(f"sst_{index}", 'r') as file: 
            file.seek(offset)

            for line in file: 
                line = line.strip() 
                inner_key, value = line.split(" ")
                
                if key == inner_key:
                    return value 
                elif key > inner_key:
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
                    self._set(key, value, True)
            
            try:
                with open(f"sst_{index_counter}.bloom", 'r') as file: 
                    line = file.read() 
                    self.bloom_filters[index_counter] = bloom_filter.BloomFilter.deserialize(line)
            except FileNotFoundError:
                print(f"Bloom filter file does not exist for index {index_counter}!")

            try:
                tuples = []

                with open(f"sst_{index_counter}.index", 'r') as file: 
                    for line in file: 
                        line = line.strip() 
                        key, offset = line.split(" ")
                        offset = int(offset)
                        tuples.append((key, offset))
                
                self.sparse_indexes[index_counter] = tuples 
            except FileNotFoundError:
                print(f"Index file does not exist for index {index_counter}!")

        self.index_counter = index_counter
        self.entries = sum(1 for v in self._store.values() if v is not config.TOMBSTONE_VALUE and v is not None)

    def _set(self, key: str, value: str, sstable_loading=False):
        prev_value = self._store.get(key)
        self._store[key] = value 

        if value is not None and prev_value is None and not sstable_loading:
            self.entries += 1
        
        if self.entries < config.MAX_ENTRIES or sstable_loading:
            return 

        # Do the flush 
        self._flush()

    def _delete(self, key: str, sstable_loading=False):
        prev_value = self._store.get(key)
        self._store[key] = config.TOMBSTONE_VALUE

        if prev_value is not None and not sstable_loading:
            self.entries -= 1

    # Public Methods 
    def set(self, key: str, value: str):
        with open(config.LOG_FILE_NAME, 'a') as file:
            file.write(f"SET {key} {value}\n")
        self._set(key, value)

    def get(self, key: str):
        raw_value = None

        if key in self._store:
            raw_value = self._store.get(key)
        else:
            raw_value = self._search_sstables(key)
        
        return None if raw_value == config.TOMBSTONE_VALUE else raw_value

    def delete(self, key: str):
        with open(config.LOG_FILE_NAME, 'a') as file:
            file.write(f"DELETE {key}\n")
        self._delete(key)

    def scan(self, start: str, end: str):
        entries = {}

        for key, value in self._store.items():
            if value == config.TOMBSTONE_VALUE:
                continue

            if key >= start and key <= end:
                entries[key] = value 

        for i in range(1, self.index_counter + 1):
            tuples = self._build_sstable_tuples(i)

            for key, value in tuples:
                if value == config.TOMBSTONE_VALUE:
                    continue

                if key >= start and key <= end:
                    entries[key] = value 

        sorted_keys = sorted(entries)
        res = []

        for key in sorted_keys:
            res.append((key, entries[key]))

        return res