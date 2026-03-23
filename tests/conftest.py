import pytest 
import src.config as config 
import src.classes.kv_store as kv_store
import os 

# This is to automatically have the "store" as a variable passed to each testing function. 
@pytest.fixture 
def store(tmp_path):
    os.chdir(tmp_path)
    return kv_store.KVStore()

@pytest.fixture 
def store(tmp_path):
    os.chdir(tmp_path)
    s = kv_store.KVStore() 
    yield s
    s.close() 

def force_flush(store, make_sure_after_normal_entries=False):
    bytes_written = 0
    i = 0

    while bytes_written < config.MAX_MEMTABLE_SIZE + 1:
        s = f"zzz_flush_{i}" if make_sure_after_normal_entries else f"foo_{i}"
        store.set(s, "bar_test")
        bytes_written += len(s) + len("bar_test")
        i += 1

    if store._flush_thread is not None:
        store._flush_thread.join()

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