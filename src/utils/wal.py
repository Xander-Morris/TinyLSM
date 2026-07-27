"""Checksum-aware binary encoding for TinyLSM write-ahead-log records."""

import binascii
import glob
import os
import shutil
import struct

from src import config

_HEADER = struct.Struct("<BQI")  # op(1 byte) + seq(8 bytes) + key_len(4 bytes)
_LEN_PREFIX = struct.Struct("<I")

_OP_CODES = {"SET": 0, "DELETE": 1}
_OP_NAMES = {0: "SET", 1: "DELETE"}

def format_wal_record(op, seq, key, value=None):
    """Encode one checksummed mutation record as a length-framed binary blob."""
    key_bytes = key.encode("utf-8")
    payload = bytearray(_HEADER.pack(_OP_CODES[op], seq, len(key_bytes)))
    payload += key_bytes

    if value is not None:
        value_bytes = value.encode("utf-8")
        payload += _LEN_PREFIX.pack(len(value_bytes))
        payload += value_bytes

    crc = binascii.crc32(payload)
    return _LEN_PREFIX.pack(len(payload)) + bytes(payload) + _LEN_PREFIX.pack(crc)

def read_wal_records(file):
    """Yield decoded records from an open binary file.

    Stops (without raising) at clean EOF, a torn write, or a bad checksum —
    same "first bad record ends replay" contract the old line parser had.
    """
    while True:
        len_bytes = file.read(4)
        if len(len_bytes) < 4:
            return
        (payload_len,) = _LEN_PREFIX.unpack(len_bytes)

        payload = file.read(payload_len)
        if len(payload) < payload_len:
            return

        crc_bytes = file.read(4)
        if len(crc_bytes) < 4:
            return
        (expected_crc,) = _LEN_PREFIX.unpack(crc_bytes)
        if binascii.crc32(payload) != expected_crc:
            return

        op_code, seq, key_len = _HEADER.unpack_from(payload, 0)
        offset = _HEADER.size
        key = payload[offset:offset + key_len].decode("utf-8")
        offset += key_len

        value = None
        if offset < len(payload):
            (value_len,) = _LEN_PREFIX.unpack_from(payload, offset)
            offset += _LEN_PREFIX.size
            value = payload[offset:offset + value_len].decode("utf-8")

        yield {"op": _OP_NAMES[op_code], "seq": seq, "key": key, "value": value}

def load_wal(store_path):
    """Splice orphaned WAL segments back onto the active log (raw byte copy)."""
    def segment_index(path):
        stem = os.path.basename(path)
        return int(stem[len("wal-"):-len(".log")])

    pending_segments = sorted(glob.glob(store_path("wal-*.log")), key=segment_index)

    for segment in pending_segments:
        with open(segment, 'rb') as src:
            with open(store_path(config.LOG_FILE_NAME), 'ab') as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fsync(dst.fileno())
        os.remove(segment)
