import pytest 
from conftest import force_flush, force_compaction, do_setting, assert_all_readable

def test_checksum_corruption(store):
    store.set("xander", "test")
    force_flush(store)