import threading 
from contextlib import contextmanager 

class ReadWriteLock:
    # Object-Specific Methods
    def __init__(self):
        self._readers = 0
        self._writing = False
        self._pending_writers = 0
        self._condition = threading.Condition()

    # Private Methods
    def _acquire_read(self):
        with self._condition:
            while self._writing or self._pending_writers > 0:
                self._condition.wait()
            self._readers += 1

    def _release_read(self):
        with self._condition:
            self._readers -= 1
            if self._readers > 0:
                return
            # Notify waiting writers that the _readers count is 0.
            self._condition.notify_all()

    def _acquire_write(self):
        with self._condition:
            self._pending_writers += 1
            while self._writing or self._readers > 0:
                self._condition.wait()
            self._pending_writers -= 1
            self._writing = True

    def _release_write(self):
        with self._condition: 
            self._writing = False 
            self._condition.notify_all()

    # Public Methods 
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