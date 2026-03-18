import pytest 
import src.classes.kv_store 
import src.config as config 
import os 

def force_flush(store):
    for i in range(config.MAX_ENTRIES):
        store.set(f"foo_{i}", "bar_test")

def force_compaction(store):
    for i in range(config.MAX_ENTRIES * config.MAX_L0_FILES + 1):
        store.set(f"compact_{i}", "bar_test")

def do_setting(store, setting):
    for key, value in setting.items():
        store.set(key, value)

def test_set_get(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    store.set("foo", "bar")
    assert store.get("foo") == "bar"

def test_set_delete_get(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    store.set("foo", "bar")
    store.delete("foo")
    assert store.get("foo") == None 

def test_tombstone_after_flush(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    store.set("foo", "bar")
    force_flush(store)
    store.delete("foo")
    force_flush(store)
    store = src.classes.kv_store.KVStore() 
    assert store.get("foo") == None 

def test_wal_replay(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)

    store = src.classes.kv_store.KVStore()

    for key, value in setting.items():
        assert store.get(key) == value 

def test_sstable_read_after_flush(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_flush(store)

    for key, value in setting.items():
        assert store.get(key) == value

def test_compaction(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)

    for key, value in setting.items():
        assert store.get(key) == value

def test_scan(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]
    setting = {"apple": "banana", "foo": "bar", "xander": "sadie", "zilophone": "wala"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]

def test_bloom_filter_false_negative(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    store.set("xander", "sadie")
    force_flush(store)
    assert store.get("xander") == "sadie" 

def test_restart_after_compaction(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)
    store = src.classes.kv_store.KVStore()

    for key, value in setting.items():
        assert store.get(key) == value

def test_overwrite_key(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    store.set("foo", "bar")
    """
        I flush between the two "set" operations here so the first value is on the disk when the second comes in,
        then assert the overwrite still wins across the SSTable boundary. 
        This tests the more interesting case where the old value is in an SSTable, and the new one is in the memtable.
    """
    force_flush(store) 
    store.set("foo", "baz")
    assert store.get("foo") == "baz"