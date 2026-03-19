import pytest 
import glob 
import src.classes.kv_store as kv_store
from conftest import force_flush, force_compaction, do_setting, assert_all_readable

def test_checksum_corruption(store):
    store.set("xander", "test")
    force_flush(store)
    files = [f for f in glob.glob("sst_*") if "." not in f]

    with open(files[0], 'r') as file: 
        lines = file.readlines() 
    
    for i, line in enumerate(lines):
        if line.startswith("xander"):
            lines[i] = lines[i][:5] + "X" + lines[i][6:]
            break

    with open(files[0], 'w') as file: 
        file.writelines(lines)

    with pytest.raises(ValueError):
        store.get("xander")

def test_manifest_corruption(store):
    with open('manifest.json', 'w') as file: 
        file.write("garbage corruption")
    
    store = kv_store.KVStore()
    assert store._manifest.entries == []