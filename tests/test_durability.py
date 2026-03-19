import pytest 
import glob 
from conftest import force_flush, force_compaction, do_setting, assert_all_readable

def test_checksum_corruption(store):
    store.set("xander", "test")
    force_flush(store)
    files = [f for f in glob.glob("sst_*") if "." not in f]

    with open(files[0], 'r') as file: 
        lines = file.readlines() 
    
    lines[0] = lines[0][:5] + "X" + lines[0][6:]

    with open(files[0], 'w') as file: 
        file.writelines(lines)

    with pytest.raises(ValueError):
        store.get("xander")