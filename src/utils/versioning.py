def pick_version(versions, at=None):
    if at is None:
        return versions[-1][1]
    for i in range(len(versions) - 1, -1, -1):
        if versions[i][0] <= at:
            return versions[i][1]
    return None