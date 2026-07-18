"""Small cross-platform wrappers for an exclusive database-directory lock."""

import sys
from contextlib import contextmanager

if sys.platform == "win32":
    import msvcrt

    def try_lock_fd(fd):
        """Try to lock one byte of a Windows file descriptor without waiting."""
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def unlock_fd(fd):
        """Release a Windows lock, ignoring an already-unlocked descriptor."""
        try:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    import fcntl

    def try_lock_fd(fd):
        """Try to take an advisory POSIX file lock without waiting."""
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def unlock_fd(fd):
        """Release an advisory POSIX file lock when it is still held."""
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass

@contextmanager
def locked_fd(fd):
    """Yield while ``fd`` is exclusively locked, or raise if it is busy."""
    if not try_lock_fd(fd):
        raise RuntimeError("Failed to acquire lock")

    try:
        yield
    finally:
        unlock_fd(fd)
