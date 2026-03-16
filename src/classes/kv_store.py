import glob 

class KVStore:
    def __init__(self, log_file_name, max_entries):
        self._store = {}
        self.log_file_name = log_file_name 
        self.max_entries = max_entries
        self.entries = 0
        self.index_counter = 0
        self._load_sstables()

    # Private Methods
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

    def _search_sstables(self, key):
        for index in range(self.index_counter, 0, -1):
            tuples = []

            with open(f"sst_{index}", 'r') as file:
                for line in file: 
                    line = line.strip() 
                    inner_key, value = line.split(" ")
                    tuples.append((inner_key, value))
            
            search_result = self._binary_search(tuples, key)

            if search_result is not None:
                return search_result
        
        return None 

    def _load_sstables(self):
        sst_file_names = glob.glob("sst_*") 
        sorted_file_names = sorted(sst_file_names, key=lambda f: int(f.split("_")[1])) # gets the index counter, like in sst_3, we get 3 and sort by that index with respect to the other files
        index_counter = 0

        for file_name in sorted_file_names:
            index_counter = int(file_name.split("_")[1])

            with open(file_name, 'r') as file:
                for line in file: 
                    line = line.strip() 
                    key, value = line.split(" ")
                    self._set(key, value, True)

        self.index_counter = index_counter
        self.entries = sum(1 for v in self._store.values() if v is not None)

    def _flush(self):
        self.index_counter += 1 # always start with incrementing by 1 to not overwrite an existing file
        sorted_store = sorted(self._store.items())

        with open(f"sst_{self.index_counter}", 'w') as file: 
            for key, value in sorted_store:
                if value is None: 
                    continue

                file.write(f"{key} {value}\n")
        
        self._store = {}
        self.entries = 0 

        with open(self.log_file_name, 'w') as file:
            file.write("")

    def _set(self, key: str, value: str, sstable_loading=False):
        prev_value = self._store.get(key)
        self._store[key] = value 

        if value is not None and prev_value is None and not sstable_loading:
            self.entries += 1
        
        if self.entries < self.max_entries or sstable_loading:
            return 

        # Do the flush 
        self._flush()

    def _delete(self, key: str):
        prev_value = self._store.get(key)
        self._store[key] = None

        if prev_value is not None:
            self.entries -= 1

    # Public Methods 
    def set(self, key: str, value: str):
        with open(self.log_file_name, 'a') as file:
            file.write(f"SET {key} {value}\n")
        self._set(key, value)

    def get(self, key: str):
        if key in self._store:
            return self._store.get(key)
        else:
            return self._search_sstables(key)

    def delete(self, key: str):
        with open(self.log_file_name, 'a') as file:
            file.write(f"DELETE {key}\n")
        self._delete(key)