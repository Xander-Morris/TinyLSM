import threading
from contextlib import contextmanager

class ReadWriteLock:
    def __init__(self):
        self._readers = 0
        self._writing = False
        self._pending_writers = 0
        self._lock = threading.Lock()
        self._readers_ok = threading.Condition(self._lock)
        self._writers_ok = threading.Condition(self._lock)

    def _acquire_read(self):
        with self._readers_ok:
            while self._writing or self._pending_writers > 0:
                self._readers_ok.wait()
            self._readers += 1

    def _release_read(self):
        with self._lock:
            self._readers -= 1
            if self._readers == 0 and self._pending_writers > 0:
                self._writers_ok.notify()

    def _acquire_write(self):
        with self._writers_ok:
            self._pending_writers += 1
            while self._writing or self._readers > 0:
                self._writers_ok.wait()
            self._pending_writers -= 1
            self._writing = True

    def _release_write(self):
        with self._lock:
            self._writing = False
            if self._pending_writers > 0:
                self._writers_ok.notify()
            else:
                self._readers_ok.notify_all()

    @contextmanager
    def read(self):
        self._acquire_read()
        try:
            yield
        finally:
            self._release_read()

    @contextmanager
    def write(self):
        self._acquire_write()
        try:
            yield
        finally:
            self._release_write()
