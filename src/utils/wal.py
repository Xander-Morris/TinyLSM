"""Checksum-aware parsing for TinyLSM write-ahead-log records."""

import binascii
import glob
import json
import os

from src import config

def parse_wal_record(line):
    """Return a decoded WAL record, or ``None`` when the line is invalid."""
    line = line.rstrip("\r\n")
    if "\t" not in line:
        return None
    payload, _, crc_str = line.rpartition("\t")
    try:
        crc = int(crc_str)
    except ValueError:
        return None
    if binascii.crc32(payload.encode("utf-8")) != crc:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None

def format_wal_record(op, seq, key, value=None):
    """Encode one checksummed mutation record for a WAL line."""
    record = {"op": op, "seq": seq, "key": key}
    if value is not None:
        record["value"] = value
    payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
    crc = binascii.crc32(payload.encode("utf-8"))
    return f"{payload}\t{crc}\n"

def load_wal(store_path):
    """Merge WAL segments orphaned by an interrupted flush back into the active log.

    ``_flush`` rotates the live WAL to ``wal-{index}.log`` and only deletes it
    once the resulting SSTable is durably published.  A crash in between
    leaves that segment behind holding data that never made it to disk any
    other way.  Splicing it back onto the head of the active log lets the
    normal startup replay recover it without a second, parallel replay path.
    """
    def segment_index(path):
        stem = os.path.basename(path)
        return int(stem[len("wal-"):-len(".log")])

    pending_segments = sorted(glob.glob(store_path("wal-*.log")), key=segment_index)

    for segment in pending_segments:
        with open(segment, 'r', encoding='utf-8') as src:
            with open(store_path(config.LOG_FILE_NAME), 'a', encoding='utf-8') as dst:
                for line in src:
                    dst.write(line)
                dst.flush()
                os.fsync(dst.fileno())
        os.remove(segment)
