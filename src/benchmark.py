import src.classes.kv_store as kv_store 
import os 
import time 
import tempfile 
import shutil 

def benchmark_writes(store, n):
    for i in range(0, n):
        store.set(f"test_key_{i}", f"test_value_{i}")

def setup():
    tempfile.mkdtemp()
    os.chdir() 
    store = kv_store.KVStore()

    return store 