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