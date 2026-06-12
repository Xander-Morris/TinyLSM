from src.utils.versioning import pick_version

def memtable_iter(table):
    for key, versions in sorted(table.items()):
        for seq, value in versions:
            yield (key, seq, value)

def get_raw_value_from_table_at(entries, key: str, at=None):
    versions = entries.get(key)
    return pick_version(versions, at)