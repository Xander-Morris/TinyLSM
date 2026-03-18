import pytest 
import src.classes.kv_store 
import src.config 
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

