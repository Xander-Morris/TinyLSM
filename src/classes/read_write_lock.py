"""A writer-preferred reader/writer lock for storage metadata and memtables."""

import threading
from contextlib import contextmanager

class ReadWriteLock:
    """Coordinate many readers with one exclusive writer.

    Writers are given priority once queued so continuous reads cannot starve a
    flush, compaction, or mutation indefinitely.
    """

    def __init__(self):
        """Create an unlocked reader/writer lock."""
        self._readers = 0
        self._writing = False
        self._pending_writers = 0
        self._lock = threading.Lock()
        self._readers_ok = threading.Condition(self._lock)
        self._writers_ok = threading.Condition(self._lock)

    def _acquire_read(self):
        """Wait until no writer is active or waiting, then register a reader."""
        with self._readers_ok:
            while self._writing or self._pending_writers > 0:
                self._readers_ok.wait()
            self._readers += 1

    def _release_read(self):
        """Release one reader and wake a waiting writer when appropriate."""
        with self._lock:
            self._readers -= 1
            if self._readers == 0 and self._pending_writers > 0:
                self._writers_ok.notify()

    def _acquire_write(self):
        """Wait until this caller is the sole active writer."""
        with self._writers_ok:
            self._pending_writers += 1
            while self._writing or self._readers > 0:
                self._writers_ok.wait()
            self._pending_writers -= 1
            self._writing = True

    def _release_write(self):
        """Release the writer and wake the next writer or all readers."""
        with self._lock:
            self._writing = False
            if self._pending_writers > 0:
                self._writers_ok.notify()
            else:
                self._readers_ok.notify_all()

    @contextmanager
    def read(self):
        """Yield while holding a shared read lock."""
        self._acquire_read()
        try:
            yield
        finally:
            self._release_read()

    @contextmanager
    def write(self):
        """Yield while holding the exclusive write lock."""
        self._acquire_write()
        try:
            yield
        finally:
            self._release_write()
