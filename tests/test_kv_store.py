import pytest 
import src.classes.kv_store 
import src.config as config 
import os 

def force_flush(store):
    for i in range(config.MAX_ENTRIES):
        store.set(f"foo_{i}", "bar_test")

def force_compaction(store):
    for i in range(config.MAX_ENTRIES * 10):
        store.set(f"foo_{i}", "bar_test")

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
    