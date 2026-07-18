"""A compact bloom filter used to avoid unnecessary SSTable reads."""

import math
import hashlib
import os
from bitarray import bitarray

class BloomFilter:
    """Probabilistic set membership with no false negatives when uncorrupted."""

    @staticmethod
    def for_capacity(n: int, false_positive_rate: float):
        """Create a filter sized for ``n`` items and a target false-positive rate."""
        if n <= 0:
            return BloomFilter(1, 1)
        m = max(1, math.ceil(-n * math.log(false_positive_rate) / (math.log(2) ** 2)))
        k = max(1, round((m / n) * math.log(2)))
        return BloomFilter(m, k)

    @staticmethod
    def deserialize(data):
        """Restore a filter from TinyLSM's human-readable on-disk format."""
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

    def _hash_index(self, key, i):
        """Map one key/hash-round pair to a bit-array position."""
        if isinstance(key, str):
            key = key.encode("utf-8")
        h = hashlib.sha256(key + i.to_bytes(4, "big")).digest()

        return int.from_bytes(h, "big") % len(self._bits)

    def __init__(self, size, num_hashes):
        """Create an empty filter with the supplied bit and hash-function counts."""
        size = max(1, size)
        num_hashes = max(1, num_hashes)
        self._bits = bitarray(size)
        self._bits.setall(0)
        self._num_hashes = num_hashes

    def add(self, key):
        """Record ``key`` as present in the filter."""
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)
            self._bits[idx] = 1

    def contains(self, key):
        """Return whether ``key`` may be present in the filter."""
        for i in range(self._num_hashes):
            idx = self._hash_index(key, i)

            if not self._bits[idx]:
                return False
            
        return True

    def serialize(self):
        """Encode the hash count and bit array for durable storage."""
        return f"{self._num_hashes}\n" + self._bits.to01()


def write_bloom_filter(store_path, index, items, false_positive_rate):
    """Build a filter sized for ``items``, persist it, and return it."""
    filter = BloomFilter.for_capacity(len(items), false_positive_rate)
    for key, _ in items:
        filter.add(key)

    with open(store_path(f"sst_{index}.bloom"), 'w', encoding='utf-8') as file:
        file.write(filter.serialize())
        file.flush()
        os.fsync(file.fileno())

    return filter

def load_bloom_filter(store_path, index):
    """Read and deserialize a bloom filter sidecar file."""
    with open(store_path(f"sst_{index}.bloom"), 'r', encoding='utf-8') as file:
        return BloomFilter.deserialize(file.read())
