import pytest 
import src.classes.kv_store 
import src.config as config 
import os 

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

    def force_flush():
        for i in range(config.MAX_ENTRIES):
            store.set(f"foo_{i}", "bar_test")
    
    force_flush()
    store.delete("foo")
    force_flush()
    store = src.classes.kv_store.KVStore() 
    assert store.get("foo") == None 

def test_wal_replay(tmp_path):
    os.chdir(tmp_path)
    store = src.classes.kv_store.KVStore()
    setting = {"foo": "bar", "xander": "sadie"}

    for key, value in setting.items():
        store.set(key, value)

    store = src.classes.kv_store.KVStore()

    for key, value in setting.items():
        assert store.get(key) == value 