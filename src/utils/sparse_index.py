"""Parsing and searching helpers for sparse SSTable index sidecars."""

import binascii
import json

def parse_sparse_index_line(line):
    """Validate and decode one checksummed sparse-index record."""
    line = line.rstrip("\r\n")
    if "\t" not in line:
        raise ValueError(f"Malformed sparse index line: {line!r}")
    payload, _, crc_str = line.rpartition("\t")
    try:
        stored_crc = int(crc_str)
    except ValueError:
        raise ValueError(f"Bad CRC in sparse index line: {line!r}")
    if binascii.crc32(payload.encode("utf-8")) != stored_crc:
        raise ValueError(f"Sparse index checksum mismatch: {line!r}")
    record = json.loads(payload)
    return record["k"], int(record["o"])

def load_sparse_index(store_path, index):
    """Read a sparse-index sidecar file into a list of ``(key, offset)`` tuples."""
    tuples = []
    with open(store_path(f"sst_{index}.index"), 'r', encoding='utf-8') as file:
        for line in file:
            if not line.strip():
                continue
            tuples.append(parse_sparse_index_line(line))
    return tuples

def search_sparse_index_for_key_offset(sparse_index, key):
    """Return the nearest indexed byte offset at or before ``key``."""
    low = 0
    high = len(sparse_index) - 1
    found = False

    while low <= high:
        mid = (low + high) // 2

        if sparse_index[mid][0] <= key:
            low = mid + 1
            found = True
        else:
            high = mid - 1

    offset = sparse_index[low - 1][1] if found else 0
    return offset
