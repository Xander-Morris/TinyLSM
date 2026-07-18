"""Selection rules for a key's sequence-numbered versions."""


def pick_version(versions, at=None):
    """Return the newest version, or the newest version no later than ``at``."""
    if at is None:
        return versions[-1][1]
    for i in range(len(versions) - 1, -1, -1):
        if versions[i][0] <= at:
            return versions[i][1]
    return None
