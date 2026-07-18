"""Read helpers for the in-memory, versioned memtable representation."""

from src.utils.versioning import pick_version

def memtable_iter(table):
    """Yield all memtable records ordered by key and then sequence number."""
    for key, versions in sorted(table.items()):
        for seq, value in versions:
            yield (key, seq, value)

def get_raw_value_from_table_at(entries, key: str, at=None):
    """Return a raw value or tombstone visible for ``key`` at ``at``."""
    versions = entries.get(key)
    return pick_version(versions, at)
