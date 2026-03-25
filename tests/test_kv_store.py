import pytest 
from conftest import force_flush, force_compaction, do_setting, assert_all_readable
import src.classes.kv_store as kv_store 

def test_set_get(store):
    store.set("foo", "bar")
    assert store.get("foo") == "bar"

def test_set_delete_get(store):
    store.set("foo", "bar")
    store.delete("foo")
    assert store.get("foo") == None 

def test_tombstone_after_flush(store):
    store.set("foo", "bar")
    force_flush(store)
    store.delete("foo")
    force_flush(store)
    store.close()
    store = kv_store.KVStore() 
    assert store.get("foo") == None 

def test_wal_replay(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    store.close()
    store = kv_store.KVStore()
    assert_all_readable(store, setting)

def test_sstable_read_after_flush(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_flush(store)
    assert_all_readable(store, setting)

def test_compaction(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)
    assert_all_readable(store, setting)

def test_scan(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]
    setting = {"apple": "banana", "foo": "bar", "xander": "sadie", "zilophone": "wala"}
    do_setting(store, setting)
    result = store.scan("foo", "xander")
    assert result == [("foo", "bar"), ("xander", "sadie")]

def test_bloom_filter_false_negative(store):
    store.set("xander", "sadie")
    force_flush(store)
    assert store.get("xander") == "sadie" 

def test_restart_after_compaction(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    force_compaction(store)
    store.close()
    store = kv_store.KVStore()
    assert_all_readable(store, setting)

def test_overwrite_key(store):
    store.set("foo", "bar")
    """
        I flush between the two "set" operations here so the first value is on the disk when the second comes in,
        then assert the overwrite still wins across the SSTable boundary. 
        This tests the more interesting case where the old value is in an SSTable, and the new one is in the memtable.
    """
    force_flush(store) 
    store.set("foo", "baz")
    assert store.get("foo") == "baz"

def test_scan_across_flush_boundary(store):
    setting1 = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting1)
    force_flush(store, True)
    setting2 = {"apple": "banana", "foo": "bar", "xander": "sadie", "zilophone": "wala"}
    do_setting(store, setting2)
    assert_all_readable(store, setting1)
    assert_all_readable(store, setting2)
    result = store.scan("apple", "zilophone")
    assert result == [("apple", "banana"), ("foo", "bar"), ("xander", "sadie"), ("zilophone", "wala")]

def test_scan_overwrite_after_flush(store):
    store.set("foo", "bar")
    force_flush(store)
    store.set("foo", "baz")
    result = store.scan("foo", "foo")
    assert result == [("foo", "baz")]

def test_scan_delete_after_flush(store):
    store.set("foo", "bar")
    force_flush(store)
    store.delete("foo")
    result = store.scan("foo", "foo")
    assert result == []

def test_stats(store):
    setting1 = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting1)
    force_flush(store)
    stats = store.stats()
    assert stats["sstable_count"] > 0
    assert stats["total_size_bytes"] > 0
    assert 0 in stats["sstables_per_level"]  
    store.set("new_key", "new_value")
    stats = store.stats()
    assert stats["memtable_keys"] >= 1
    assert stats["memtable_size_bytes"] > 0

def test_sequence_behavior(store):
    store.set("foo", "bar")
    seq = store._seq 
    store.set("foo", "baz")
    assert store.get("foo") == "baz"
    assert store.get("foo", at=seq) == "bar"
    assert store.get("foo", at=seq - 1) == None

def test_snapshot_after_flush(store):
    store.set("foo", "bar")
    seq = store._seq
    store.set("foo", "baz")
    force_flush(store)
    assert store.get("foo") == "baz"
    assert store.get("foo", at=seq) == "bar"
    assert store.get("foo", at=seq - 1) == None

def test_scan_snapshot_after_flush(store):
    store.set("foo", "bar")
    seq1 = store._seq 
    force_flush(store)
    store.set("foo", "baz")
    assert store.scan("foo", "foo", at=seq1) == [('foo', 'bar')]
    assert store.scan("foo", "foo") == [('foo', 'baz')]