import pytest 
import src.classes.kv_store 
import src.config as config 
import os 

def force_flush(store, make_sure_after_normal_entries=False):
    bytes_written = 0
    i = 0

    while bytes_written < config.MAX_MEMTABLE_SIZE + 1:
        s = f"zzz_flush_{i}" if make_sure_after_normal_entries else f"foo_{i}"
        store.set(s, "bar_test")
        bytes_written += len(s) + len("bar_test")
        i += 1

def force_compaction(store):
    bytes_written = 0
    i = 0
    
    while bytes_written < (config.MAX_MEMTABLE_SIZE + 1) * (config.MAX_L0_FILES + 1):
        s = f"compact_{i}"
        store.set(s, "bar_test")
        bytes_written += len(s) + len("bar_test")
        i += 1

def do_setting(store, setting):
    for key, value in setting.items():
        store.set(key, value)

def assert_all_readable(store, setting):
    for key, value in setting.items():
        assert store.get(key) == value

# This is to automatically have the "store" as a variable passed to each testing function. 
@pytest.fixture 
def store(tmp_path):
    os.chdir(tmp_path)
    return src.classes.kv_store.KVStore()

def test_set_get(store):
    store.set("foo", "bar")
    assert store.get("foo") == "bar"

def test_set_delete_get(store):
    store.set("foo", "bar")
    store.delete("foo")
    assert store.get("foo") == None 

def test_tombstone_after_flush(store):
    store.set("foo", "bar")
    force_flush(store)
    store.delete("foo")
    force_flush(store)
    store.close()
    store = src.classes.kv_store.KVStore() 
    assert store.get("foo") == None 

def test_wal_replay(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    store.close()
    store = src.classes.kv_store.KVStore()
    assert_all_readable(store, setting)

def test_sstable_read_after_flush(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_flush(store)
    assert_all_readable(store, setting)

def test_compaction(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)
    assert_all_readable(store, setting)

def test_scan(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]
    setting = {"apple": "banana", "foo": "bar", "xander": "sadie", "zilophone": "wala"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]

def test_bloom_filter_false_negative(store):
    store.set("xander", "sadie")
    force_flush(store)
    assert store.get("xander") == "sadie" 

def test_restart_after_compaction(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)
    store.close()
    store = src.classes.kv_store.KVStore()
    assert_all_readable(store, setting)

def test_overwrite_key(store):
    store.set("foo", "bar")
    """
        I flush between the two "set" operations here so the first value is on the disk when the second comes in,
        then assert the overwrite still wins across the SSTable boundary. 
        This tests the more interesting case where the old value is in an SSTable, and the new one is in the memtable.
    """
    force_flush(store) 
    store.set("foo", "baz")
    assert store.get("foo") == "baz"

def test_scan_across_flush_boundary(store):
    setting1 = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting1)
    force_flush(store, True)
    setting2 = {"apple": "banana", "foo": "bar", "xander": "sadie", "zilophone": "wala"}
    do_setting(store, setting2)
    assert_all_readable(store, setting1)
    assert_all_readable(store, setting2)
    result = store.scan("apple", "zilophone")
    assert result == [("apple", "banana"), ("foo", "bar"), ("xander", "sadie"), ("zilophone", "wala")]