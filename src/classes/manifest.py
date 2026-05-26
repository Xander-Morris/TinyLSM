import json
import os


class Manifest:
    # Static Methods
    @staticmethod
    def load(data_dir):
        path = os.path.join(data_dir, "manifest.json")
        try:
            with open(path, 'r') as file:
                lst = json.load(file)
                obj = Manifest(data_dir)
                obj.entries = lst

                return obj
        except (FileNotFoundError, json.JSONDecodeError):
            return Manifest(data_dir)

    def __init__(self, data_dir):
        self._data_dir = data_dir
        self.entries = []

    # Public Methods
    def add(self, level, file_name, min_key, max_key):
        self.entries.append({"level": level, "file_name": file_name, "min_key": min_key, "max_key": max_key})

    def remove(self, file_name):
        self.entries = [entry for entry in self.entries if entry["file_name"] != file_name]

    def save(self):
        tmp_path = os.path.join(self._data_dir, "manifest.tmp")
        target_path = os.path.join(self._data_dir, "manifest.json")
        with open(tmp_path, 'w') as file:
            json.dump(self.entries, file)
            file.flush()
            os.fsync(file.fileno())
        # Atomic on Windows and Linux, so it can never be in a partial state.
        os.replace(tmp_path, target_path)

    def clear(self):
        self.entries = []
