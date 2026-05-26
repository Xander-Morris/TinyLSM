import binascii
import json
import os


def _canonical_entries(entries):
    return json.dumps(entries, sort_keys=True, separators=(",", ":")).encode("utf-8")


class Manifest:
    @staticmethod
    def load(data_dir):
        path = os.path.join(data_dir, "manifest.json")
        try:
            with open(path, 'r', encoding='utf-8') as file:
                obj = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return Manifest(data_dir)

        if not isinstance(obj, dict) or "entries" not in obj or "crc" not in obj:
            raise ValueError(f"Manifest at {path} is missing required envelope fields")

        entries = obj["entries"]
        stored_crc = obj["crc"]
        computed_crc = binascii.crc32(_canonical_entries(entries))
        if stored_crc != computed_crc:
            raise ValueError(f"Manifest checksum mismatch: stored {stored_crc}, computed {computed_crc}")

        m = Manifest(data_dir)
        m.entries = entries
        return m

    def __init__(self, data_dir):
        self._data_dir = data_dir
        self.entries = []

    def add(self, level, file_name, min_key, max_key):
        self.entries.append({"level": level, "file_name": file_name, "min_key": min_key, "max_key": max_key})

    def remove(self, file_name):
        self.entries = [entry for entry in self.entries if entry["file_name"] != file_name]

    def save(self):
        tmp_path = os.path.join(self._data_dir, "manifest.tmp")
        target_path = os.path.join(self._data_dir, "manifest.json")
        crc = binascii.crc32(_canonical_entries(self.entries))
        envelope = {"crc": crc, "entries": self.entries}
        with open(tmp_path, 'w', encoding='utf-8') as file:
            json.dump(envelope, file)
            file.flush()
            os.fsync(file.fileno())
        # Atomic on Windows and Linux, so it can never be in a partial state.
        os.replace(tmp_path, target_path)

    def clear(self):
        self.entries = []
