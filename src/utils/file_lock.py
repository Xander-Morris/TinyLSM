import sys
from contextlib import contextmanager

if sys.platform == "win32":
    import msvcrt

    def try_lock_fd(fd):
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def unlock_fd(fd):
        try:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    import fcntl

    def try_lock_fd(fd):
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def unlock_fd(fd):
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass

@contextmanager
def locked_fd(fd):
    if not try_lock_fd(fd):
        raise RuntimeError("Failed to acquire lock")

    try:
        yield
    finally:
        unlock_fd(fd)