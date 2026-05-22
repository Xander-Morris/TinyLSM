import math
import hashlib
from bitarray import bitarray 

class BloomFilter:
    @staticmethod
    def for_capacity(n, false_positive_rate):
        m = math.ceil(-n * math.log(false_positive_rate) / (math.log(2) ** 2))
        k = max(1, round((m / n) * math.log(2)))
        return BloomFilter(max(m, 1), k)

    @staticmethod
    def deserialize(data):
        num_hashes, bits_str = data.strip().split("\n", 1)
        f = BloomFilter(len(bits_str), int(num_hashes))
        for i, char in enumerate(bits_str):
            f._bits[i] = int(char)
        return f
    
    # Private Helpers
    def _hash_index(self, key, i):
        if isinstance(key, str):
            key = key.encode()
        h = hashlib.sha256(key + i.to_bytes(2, "big")).digest()

        return int.from_bytes(h, "big") % len(self._bits)

    def __init__(self, size, num_hashes):
        self._bits = bitarray([0] * size)
        self._num_hashes = num_hashes

    def add(self, key):
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)
            self._bits[idx] = 1

    def contains(self, key):
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)
            if self._bits[idx] != 1:
                return False
        return True

    def serialize(self):
        return f"{self._num_hashes}\n" + "".join(str(b) for b in self._bits)