"""A compact bloom filter used to avoid unnecessary SSTable reads."""

import binascii
import math
import hashlib
import os
import struct
from bitarray import bitarray

# Binary sidecar layout: magic, format version, hash count, bit count, CRC32 of
# the packed bits, then the bits themselves.
_MAGIC = b"TLBF"
_FORMAT_VERSION = 2
_HEADER = struct.Struct("<4sBIQI")

class BloomFilter:
    """Probabilistic set membership with no false negatives when uncorrupted.

    Positions come from one BLAKE2b digest split into two 64-bit hashes and
    combined with double hashing, so each key costs a single
    hash no matter how many positions it sets.

    Filters written before the binary format used one SHA-256 per position.
    Those are still loaded with their original hashing (legacy).
    Reading them with the new scheme would produce false negatives.
    """

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
        """Restore a filter from the binary format or the legacy text format."""
        if isinstance(data, (bytes, bytearray)) and data[:len(_MAGIC)] == _MAGIC:
            return BloomFilter._deserialize_binary(data)
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        return BloomFilter._deserialize_legacy_text(data)

    @staticmethod
    def _deserialize_binary(data):
        """Validate and decode a checksummed binary bloom filter."""
        if len(data) < _HEADER.size:
            raise ValueError("Malformed bloom filter data: truncated header")
        _, version, num_hashes, num_bits, stored_crc = _HEADER.unpack_from(data, 0)
        if version != _FORMAT_VERSION:
            raise ValueError(f"Unsupported bloom filter format version: {version}")
        body = bytes(data[_HEADER.size:])
        if binascii.crc32(body) != stored_crc:
            raise ValueError("Bloom filter checksum mismatch")
        if num_hashes < 1 or num_bits < 1 or len(body) != (num_bits + 7) // 8:
            raise ValueError("Malformed bloom filter data: bad sizes")

        f = BloomFilter(num_bits, num_hashes)
        bits = bitarray(endian="little")
        bits.frombytes(body)
        del bits[num_bits:]
        f._bits = bits
        return f

    @staticmethod
    def _deserialize_legacy_text(data):
        """Restore a filter from the original human-readable on-disk format."""
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
        f = BloomFilter(len(bits_str), num_hashes, legacy=True)
        f._bits = bitarray(bits_str, endian="little")
        return f

    def __init__(self, size, num_hashes, legacy=False):
        """Create an empty filter with the supplied bit and hash-function counts."""
        self._size = max(1, size)
        self._num_hashes = max(1, num_hashes)
        self._legacy = legacy
        self._bits = bitarray(self._size, endian="little")
        self._bits.setall(0)

    def _positions(self, key):
        """Return the bit positions ``key`` maps to."""
        if isinstance(key, str):
            key = key.encode("utf-8")

        if self._legacy:
            return [
                int.from_bytes(hashlib.sha256(key + i.to_bytes(4, "big")).digest(), "big") % self._size
                for i in range(self._num_hashes)
            ]

        digest = hashlib.blake2b(key, digest_size=16).digest()
        h1 = int.from_bytes(digest[:8], "little")
        h2 = int.from_bytes(digest[8:], "little") | 1  # odd, so positions never collapse to one
        size = self._size
        return [(h1 + i * h2) % size for i in range(self._num_hashes)]

    def add(self, key):
        """Record ``key`` as present in the filter."""
        bits = self._bits
        for idx in self._positions(key):
            bits[idx] = 1

    def contains(self, key):
        """Return whether ``key`` may be present in the filter."""
        bits = self._bits
        for idx in self._positions(key):
            if not bits[idx]:
                return False

        return True

    def serialize(self):
        """Encode the filter for durable storage."""
        if self._legacy:
            return (f"{self._num_hashes}\n" + self._bits.to01()).encode("utf-8")
        body = self._bits.tobytes()
        header = _HEADER.pack(_MAGIC, _FORMAT_VERSION, self._num_hashes, self._size, binascii.crc32(body))
        return header + body


def write_bloom_filter(store_path, index, items, false_positive_rate):
    """Build a filter sized for ``items``, persist it, and return it.

    ``items`` may be a dict of key -> versions (the flush path) or an
    iterable of ``(key, versions)`` pairs (the compaction path).
    """
    filter = BloomFilter.for_capacity(len(items), false_positive_rate)
    pairs = items.items() if hasattr(items, "items") else items
    for key, _ in pairs:
        filter.add(key)

    with open(store_path(f"sst_{index}.bloom"), 'wb') as file:
        file.write(filter.serialize())
        file.flush()
        os.fsync(file.fileno())

    return filter

def load_bloom_filter(store_path, index):
    """Read and deserialize a bloom filter sidecar file."""
    with open(store_path(f"sst_{index}.bloom"), 'rb') as file:
        return BloomFilter.deserialize(file.read())
