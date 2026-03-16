class KVStore:
    def __init__(self):
        self._store = {}

    def set(self, key: str, value: str):
        self._store[key] = value

    def get(self, key: str):
        return self._store.get(key)

    def delete(self, key: str):
        self._store[key] = None