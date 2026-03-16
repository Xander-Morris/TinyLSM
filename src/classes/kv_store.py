class KVStore:
    def __init__(self, log_file_name):
        self._store = {}
        self.log_file_name = log_file_name 

    # Private Methods
    def _set(self, key: str, value: str):
        self._store[key] = value 

    def _delete(self, key: str):
        self._store[key] = None

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