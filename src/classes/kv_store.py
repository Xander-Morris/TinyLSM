class KVStore:
    def __init__(self, log_file_name):
        self._store = {}
        self.log_file_name = log_file_name 

    def set(self, key: str, value: str):
        try:
            with open(self.log_file_name, 'w') as file:
                file.write(f"SET {key} {value}")
                file.close()
                self._store[key] = value
        except:
            print("No file exists!")

    def get(self, key: str):
        return self._store.get(key)

    def delete(self, key: str):
        try:
            with open(self.log_file_name, 'w') as file:
                file.write(f"DELETE {key}")
                file.close()
                self._store[key] = None
        except:
            print("No file exists!")