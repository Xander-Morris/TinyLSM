import src.classes.kv_store as kv_store 
import os 
import time 
import tempfile 
import shutil 

def benchmark_writes(store, n):
    start = time.perf_counter()
    for i in range(0, n):
        store.set(f"test_key_{i}", f"test_value_{i}")
    end = time.perf_counter()
    
    return end - start 

def setup():
    pth = tempfile.mkdtemp()
    os.chdir(pth) 
    store = kv_store.KVStore()

    return store 