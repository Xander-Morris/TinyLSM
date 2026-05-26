import pytest 
import glob 
import json
import os 
import src.classes.kv_store as kv_store
from conftest import force_flush

def test_checksum_corruption(store):
    store.set("xander", "test")
    force_flush(store)
    files = [f for f in glob.glob("sst_*") if "." not in f]

    with open(files[0], 'r') as file: 
        lines = file.readlines() 
    
    for i, line in enumerate(lines):
        if line.strip():
            lines[i] = line.replace('"k"', '"K"', 1)
            break

    with open(files[0], 'w') as file: 
        file.writelines(lines)

    with pytest.raises(ValueError):
        store.get("xander")

def test_manifest_corruption(store):
    with open('manifest.json', 'w') as file: 
        file.write("garbage corruption")
    
    store.close()
    reopened = kv_store.KVStore()
    try:
        assert reopened._manifest.entries == []
    finally:
        reopened.close()

def test_lockfile_prevents_second_open(store):
    with pytest.raises(RuntimeError):
        kv_store.KVStore()

def test_sparse_index_checksum_corruption(store):
    store.set("xander", "test")
    force_flush(store)
    index_files = glob.glob("sst_*.index")

    with open(index_files[0], 'r') as file:
        lines = file.readlines()

    for i, line in enumerate(lines):
        if line.strip():
            lines[i] = line.replace('"k"', '"K"', 1)
            break
    else:
        pytest.fail("expected sparse index entries")

    with open(index_files[0], 'w') as file:
        file.writelines(lines)

    store.close()
    with pytest.raises(ValueError):
        kv_store.KVStore()

def test_meta_file_persists_flush_counters(store):
    store.set("xander", "test")
    force_flush(store)
    stats = store.stats()

    with open("meta", 'r') as file:
        meta = json.load(file)

    assert meta["seq"] == store._seq
    assert meta["bytes_written_user"] == stats["bytes_written_user"]
    assert meta["bytes_written_disk"] == stats["bytes_written_disk"]

def test_manifest_atomic_write(store):
    store.set("xander", "test")
    force_flush(store)
    
    with open("manifest.json", 'r') as file: 
        json.load(file) 

    assert not os.path.exists("manifest.tmp")

def test_checksum_valid_after_restart(store):
    store.set("xander", "test")
    force_flush(store)
    store.close() 
    store = kv_store.KVStore() 
    assert store.get("xander") == "test"
