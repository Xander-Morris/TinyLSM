import math
import hashlib
from bitarray import bitarray

class BloomFilter:
    @staticmethod
    def for_capacity(n: int, false_positive_rate: int):
        """
            I use this to determine the optimal bit-array size (m) and number of hash functions (k) to use to store
            a specific number of items (n) while maintaining a target false positive rate.  
            
            Args:
                n: number of items to store 
                false_positive_rate: target false positive rate for entire bloom filter
        """
        if n <= 0:
            return BloomFilter(1, 1)
        m = max(1, math.ceil(-n * math.log(false_positive_rate) / (math.log(2) ** 2)))
        k = max(1, round((m / n) * math.log(2)))
        return BloomFilter(m, k)

    @staticmethod
    def deserialize(data):
        data = data.strip()
        if "\n" not in data:
            raise ValueError("Malformed bloom filter data: missing newline separator")
        num_hashes_str, bits_str = data.split("\n", 1)
        try:
            num_hashes = int(num_hashes_str)
        except ValueError:
            raise ValueError(f"Bloom filter has invalid num_hashes: {num_hashes_str!r}")
        if not bits_str:
            raise ValueError("Bloom filter has empty bit string")
        if any(c not in "01" for c in bits_str):
            raise ValueError("Bloom filter has non-binary char in bit string")
        f = BloomFilter(len(bits_str), num_hashes)
        f._bits = bitarray(bits_str)
        return f

    # Private Helpers
    def _hash_index(self, key, i):
        if isinstance(key, str):
            key = key.encode("utf-8")
        h = hashlib.sha256(key + i.to_bytes(4, "big")).digest()

        return int.from_bytes(h, "big") % len(self._bits)

    def __init__(self, size, num_hashes):
        size = max(1, size)
        num_hashes = max(1, num_hashes)
        self._bits = bitarray(size)
        self._bits.setall(0)
        self._num_hashes = num_hashes

    def add(self, key):
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)
            self._bits[idx] = 1

    def contains(self, key):
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)

            if not self._bits[idx]:
                return False
            
        return True

    def serialize(self):
        return f"{self._num_hashes}\n" + self._bits.to01()