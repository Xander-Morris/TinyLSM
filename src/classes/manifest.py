import json

class Manifest: 
    # Static Methods
    @staticmethod 
    def load(): 
        try:
            with open("manifest.json", 'r') as file: 
                lst = json.load(file)
                obj = Manifest()
                obj.entries = lst 

                return obj 
        except FileNotFoundError:
            return Manifest()

    def __init__(self):
        self.entries = []

    # Public Methods
    def add(self, level, file_name, min_key, max_key):
        self.entries.append({"level": level, "file_name": file_name, "min_key": min_key, "max_key": max_key})

    def remove(self, file_name):
        self.entries = [entry for entry in self.entries if entry["file_name"] != file_name]
    
    def save(self):
        with open("manifest.json", 'w') as file: 
            json.dump(self.entries, file)