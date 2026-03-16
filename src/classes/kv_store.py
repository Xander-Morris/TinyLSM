class KVStore:
    def __init__(self, log_file_name, max_entries):
        self._store = {}
        self.log_file_name = log_file_name 
        self.max_entries = max_entries
        self.entries = 0

    # Private Methods
    def _flush(self):


    def _set(self, key: str, value: str):
        prev_value = self._store.get(key)
        self._store[key] = value 

        if not prev_value:
            self.entries += 1
        
        if self.entries < self.max_entries:
            return 

        # Do the flush 
        self._flush()

    def _delete(self, key: str):
        prev_value = self._store.get(key)
        self._store[key] = None

        if prev_value:
            self.entries -= 1

    # Public Methods 
    def set(self, key: str, value: str):
        with open(self.log_file_name, 'a') as file:
            file.write(f"SET {key} {value}\n")
        self._set(key, value)

    def get(self, key: str):
        return self._store.get(key)

    def delete(self, key: str):
        with open(self.log_file_name, 'a') as file:
            file.write(f"DELETE {key}\n")
        self._delete(key)