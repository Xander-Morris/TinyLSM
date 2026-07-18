"""SSTable record parsing and fallback searching utilities."""

import binascii
import glob
import json
import os

from src import config
from src.classes.tombstone import TombstoneType
from src.utils.sparse_index import search_sparse_index_for_key_offset
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

def write_to_sstable_file(store_path, index, sorted_store):
    """Write one sorted SSTable and its sparse-index sidecar durably."""
    sparse = []
    min_key, max_key = None, None

    with open(store_path(f"sst_{index}"), 'w', encoding='utf-8') as file:
        key_count = 0

        for key, versions in sorted_store:
            key_count += 1
            first_version = True

            for seq, value in versions:
                if min_key is None:
                    min_key = key
                max_key = key
                offset = file.tell()
                record = {"k": key, "s": seq}
                if value is _TOMBSTONE:
                    record["t"] = True
                else:
                    record["v"] = value
                payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
                checksum = binascii.crc32(payload.encode("utf-8"))
                file.write(f"{payload}\t{checksum}\n")

                if first_version and key_count % config.SPARSE_INDEX_N == 0:
                    sparse.append((key, offset))
                    first_version = False

        file.flush()
        os.fsync(file.fileno())

    with open(store_path(f"sst_{index}.index"), 'w', encoding='utf-8') as file:
            for key, offset in sparse:
                record = {"k": key, "o": offset}
                payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
                crc = binascii.crc32(payload.encode("utf-8"))
                file.write(f"{payload}\t{crc}\n")
            file.flush()
            os.fsync(file.fileno())

    return (sparse, min_key, max_key)

def read_sstable_tuples(store_path, index):
    """Read one SSTable file into a list of ``(key, seq, value)`` tuples."""
    tuples = []
    with open(store_path(f"sst_{index}"), 'r', encoding='utf-8') as file:
        for line in file:
            if not line.strip():
                continue
            tuples.append(parse_sstable_line(line))
    return tuples

def iter_sstable_from(store_path, index, sparse_index, start_key):
    """Yield records from an SSTable, seeking near ``start_key`` first."""
    offset = search_sparse_index_for_key_offset(sparse_index, start_key)

    with open(store_path(f"sst_{index}"), 'r', encoding='utf-8') as file:
        file.seek(offset)

        for line in file:
            key, seq, value = parse_sstable_line(line)

            if key < start_key:
                continue
            yield (key, seq, value)

def search_sstable_with_index(store_path, index, sparse_index, key, at=None):
    """Find the version of ``key`` visible at ``at`` in one SSTable."""
    offset = search_sparse_index_for_key_offset(sparse_index, key)
    versions = []

    with open(store_path(f"sst_{index}"), 'r', encoding='utf-8') as file:
        file.seek(offset)

        for line in file:
            inner_key, seq, value = parse_sstable_line(line)

            if key == inner_key:
                versions.append((seq, value))
            elif key < inner_key:
                break

    return pick_version(versions, at) if versions else None

def cleanup_orphan_sst_files(store_path, expected_indices):
    """Delete SSTable sidecars on disk that aren't in ``expected_indices``.

    A crash before a manifest update can leave fully written but unpublished
    files behind.  The manifest is the source of truth for published data.
    """
    on_disk = set()
    for path in glob.glob(store_path("sst_*")):
        base = os.path.basename(path)
        stem = base.split(".", 1)[0]
        try:
            on_disk.add(int(stem.split("_", 1)[1]))
        except (ValueError, IndexError):
            continue

    for idx in on_disk - expected_indices:
        for ext in ("", ".bloom", ".index"):
            try:
                os.remove(store_path(f"sst_{idx}{ext}"))
            except FileNotFoundError:
                pass