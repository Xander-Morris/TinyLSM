import hashlib

import pytest

from src.classes.bloom_filter import BloomFilter

def test_bloom_serialize_deserialize():
    bf = BloomFilter.for_capacity(100, 0.01)
    keys = [b"foo", b"bar", b"baz"]
    for k in keys:
        bf.add(k)

    s = bf.serialize()
    bf2 = BloomFilter.deserialize(s)

    for k in keys:
        assert bf2.contains(k)

    assert not bf2.contains(b"unknown_key")

def test_bloom_accepts_str_keys():
    bf = BloomFilter.for_capacity(50, 0.05)
    bf.add("alice")
    assert bf.contains("alice")
    assert not bf.contains("bob")

def test_bloom_binary_checksum_corruption():
    bf = BloomFilter.for_capacity(100, 0.01)
    bf.add("alice")
    data = bytearray(bf.serialize())
    data[-1] ^= 0xFF

    with pytest.raises(ValueError):
        BloomFilter.deserialize(bytes(data))

def test_bloom_legacy_text_format_keeps_original_hashing():
    """Sidecars written before the binary format must not produce false negatives."""
    size, num_hashes = 512, 4
    keys = ["alice", "bob", "carol"]
    bits = ["0"] * size
    for key in keys:
        for i in range(num_hashes):
            digest = hashlib.sha256(key.encode("utf-8") + i.to_bytes(4, "big")).digest()
            bits[int.from_bytes(digest, "big") % size] = "1"
    legacy = f"{num_hashes}\n{''.join(bits)}".encode("utf-8")

    bf = BloomFilter.deserialize(legacy)
    for key in keys:
        assert bf.contains(key)

    reloaded = BloomFilter.deserialize(bf.serialize())
    for key in keys:
        assert reloaded.contains(key)