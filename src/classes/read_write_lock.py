import threading 
from contextlib import contextmanager 

class ReadWriteLock:
    def __init__(self):
        self._readers = 0
        self._writing = False 
        self._condition = threading.Condition()

    def acquire_read(self):
        with self._condition: 
            while self._writing:
                self._condition.wait()
            self._readers += 1 

    def release_read(self):
        with self._condition: 
            self._readers -= 1
            if self._readers > 0:
                return
            # Notify waiting writers that the _readers count is 0.
            self._condition.notify_all()

    def acquire_write(self):
        with self._condition: 
            while self._writing or self._readers > 0:
                self._condition.wait()
            self._writing = True 

    def release_write(self):
        with self._condition: 
            self._writing = False 
            self._condition.notify_all()

    @contextmanager 
    def read(self):
        self.acquire_read()

        try:
            yield 
        finally:
            self.release_read()

    @contextmanager
    def write(self):
        self.acquire_write()

        try:
            yield 
        finally:
            self.release_write()