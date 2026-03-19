import src.config as config 

class BloomFilter: 
    # Static Methods
    @staticmethod
    def deserialize(data):
        filter = BloomFilter(len(data))
        index = 0

        for char in data: 
            i = int(char) 
            filter._bits[index] = i 
            index += 1
        
        return filter 

    # Object-Specific Methods 
    def __init__(self, size):
        self._bits = [0] * size 
    
    # Public Methods 
    def add(self, key):
        for i in range(config.HASH_FUNCTIONS):
            index = hash(key + str(i)) % len(self._bits)
            self._bits[index] = 1

    def contains(self, key):
        for i in range(config.HASH_FUNCTIONS):
            index = hash(key + str(i)) % len(self._bits)
            
            if self._bits[index] != 1:
                return False 
        
        return True
    
    def serialize(self):
        return "".join(str(b) for b in self._bits)