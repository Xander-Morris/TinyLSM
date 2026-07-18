"""SSTable record parsing and fallback searching utilities."""

import binascii
import json

from src.classes.tombstone import TombstoneType
from src.utils.versioning import pick_version

_TOMBSTONE = TombstoneType()

def sst_index(entry):
    """Extract TinyLSM's numeric SSTable generation from a manifest entry."""
    return int(entry["file_name"].split("_")[1])

def parse_sstable_line(line):
    """Validate and decode one checksummed SSTable record."""
    line = line.rstrip("\r\n")
    if "\t" not in line:
        raise ValueError(f"Malformed SSTable line: {line!r}")
    payload, _, crc_str = line.rpartition("\t")
    try:
        stored_crc = int(crc_str)
    except ValueError:
        raise ValueError(f"Bad CRC field in SSTable line: {line!r}")
    computed_crc = binascii.crc32(payload.encode("utf-8"))
    if stored_crc != computed_crc:
        raise ValueError(f"Checksum mismatch: expected {computed_crc}, got {stored_crc}")
    record = json.loads(payload)
    value = _TOMBSTONE if record.get("t") else record["v"]
    return record["k"], int(record["s"]), value

def binary_search(tuples, key, at=None):
    """Find the version of ``key`` visible at ``at`` in parsed SSTable tuples."""
    low = 0
    high = len(tuples) - 1

    while low <= high:
        mid = (low + high) // 2
        key_at_mid = tuples[mid][0]

        if key == key_at_mid:
            versions = [(seq, value) for k, seq, value in tuples if k == key]
            return pick_version(versions, at)
        elif key < key_at_mid:
            high = mid - 1
        else:
            low = mid + 1

    return None
