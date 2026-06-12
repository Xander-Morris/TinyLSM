import binascii
import json

def parse_wal_record(line):
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