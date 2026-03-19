import pytest 

def test_checksum_corruption(store):
    store.set("xander", "test")
    force_flush()