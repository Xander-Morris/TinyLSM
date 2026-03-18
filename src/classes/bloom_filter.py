import src.config as config 

class BloomFilter: 
    # Static Methods
    @staticmethod
    def deserialize(data):
        filter = BloomFilter(len(data))
        index = 0

        for char in data: 
            i = int(char) 
            filter.bits[index] = i 
            index += 1
        
        return filter 

    def __init__(self, size):
        self.bits = [0] * size 
    
    # Public Methods 
    def add(self, key):
        for i in range(config.HASH_FUNCTIONS):
            index = hash(key + str(i)) % len(self.bits)
            self.bits[index] = 1

    def contains(self, key):
        for i in range(config.HASH_FUNCTIONS):
            index = hash(key + str(i)) % len(self.bits)
            
            if self.bits[index] != 1:
                return False 
        
        return True
    
    def serialize(self):
        return "".join(str(b) for b in self.bits)