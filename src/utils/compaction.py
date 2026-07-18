"""Helpers for sizing and splitting compaction output SSTables."""

from src.classes.tombstone import TombstoneType

_TOMBSTONE = TombstoneType()
_TOMBSTONE_BYTES = 1  # Accounting weight for tombstone marker in memtable

def versions_size(key, versions):
    """Estimate the in-memory payload size of every version for one key."""
    total = 0
    for _, value in versions:
        val_size = _TOMBSTONE_BYTES if value is _TOMBSTONE else len(value)
        total += len(key) + val_size
    return total

def chunk_by_target_size(sorted_items, target_size):
    """Split sorted key/version groups into SSTable-sized chunks.

    A key's complete version history stays in a single chunk so an SSTable
    never contains only part of that key's visible history.
    """
    chunks = []
    chunk = []
    chunk_size = 0
    target_size = max(1, target_size)

    for key, versions in sorted_items:
        item_size = versions_size(key, versions)
        if chunk and chunk_size + item_size > target_size:
            chunks.append(chunk)
            chunk = []
            chunk_size = 0
        chunk.append((key, versions))
        chunk_size += item_size

    if chunk:
        chunks.append(chunk)

    return chunks
