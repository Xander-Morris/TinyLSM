import pytest 
from conftest import force_flush, force_compaction, do_setting, assert_all_readable
import src.config as config
import src.classes.kv_store as kv_store 


def _flush_active_memtable(store, name):
    """Force one small, deterministic flush for compaction tests."""
    store.set(f"__flush_{name}", "x" * config.MAX_MEMTABLE_SIZE)
    assert store._flush_thread is not None
    store._flush_thread.join()

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

def test_write_amplification(store):
    n = 10 
    
    for i in range(n):
        store.set(f"{i}", f"{i}")
    
    force_flush(store)
    assert store.stats()["write_amplification"] > 1.0

def test_iter(store):
    setting = {"foo": "bar", "xander": "sadie"}
    do_setting(store, setting)
    result = list(store.iter("foo", "xander"))
    assert result == [("foo", "bar"), ("xander", "sadie")]

def test_iter_materializes_at_call_time(store):
    store.set("foo", "bar")
    result = store.iter("foo", "xander")
    store.set("xander", "sadie")
    assert list(result) == [("foo", "bar")]

def test_iter_snapshot_after_flush(store):
    store.set("foo", "bar")
    seq1 = store._seq
    force_flush(store)
    store.set("foo", "baz")
    assert list(store.iter("foo", "foo", at=seq1)) == [("foo", "bar")]
    assert list(store.iter("foo", "foo")) == [("foo", "baz")]


def test_pinned_snapshot_survives_compaction_and_is_reclaimed_after_close(store, monkeypatch):
    """Compaction keeps history only while a snapshot has pinned its sequence."""
    monkeypatch.setattr(config, "MAX_MEMTABLE_SIZE", 64)
    monkeypatch.setattr(config, "MAX_L0_FILES", 2)

    store.set("history", "v1")
    with store.snapshot() as snapshot:
        store.set("history", "v2")
        _flush_active_memtable(store, "first")
        store.set("history", "v3")
        _flush_active_memtable(store, "second")

        assert store.get("history") == "v3"
        assert store.get("history", at=snapshot) == "v1"
        assert store.scan("history", "history", at=snapshot) == [("history", "v1")]

    assert not store._active_snapshots

    # Two more L0 files trigger another compaction.  With no snapshot pinned,
    # only the newest version has to remain.
    store.set("history", "v4")
    _flush_active_memtable(store, "third")
    store.set("history", "v5")
    _flush_active_memtable(store, "fourth")

    assert store.get("history") == "v5"
    assert store.get("history", at=snapshot) is None


def test_pinned_snapshot_preserves_value_before_a_compacted_delete(store, monkeypatch):
    """A tombstone remains while a snapshot can still see the prior value."""
    monkeypatch.setattr(config, "MAX_MEMTABLE_SIZE", 64)
    monkeypatch.setattr(config, "MAX_L0_FILES", 2)

    store.set("deleted", "before")
    with store.snapshot() as snapshot:
        store.delete("deleted")
        _flush_active_memtable(store, "delete")
        _flush_active_memtable(store, "compact")

        assert store.get("deleted") is None
        assert store.get("deleted", at=snapshot) == "before"


def test_snapshots_at_the_same_sequence_are_reference_counted(store):
    """Closing one of two equal snapshots must not unpin the other one."""
    store.set("key", "value")

    with store.snapshot() as first:
        with store.snapshot() as second:
            assert first == second
            assert store._active_snapshots[first] == 2
            assert store._oldest_active_snapshot_seq() == first

        assert store._active_snapshots[first] == 1
        assert store._oldest_active_snapshot_seq() == first

    assert not store._active_snapshots
