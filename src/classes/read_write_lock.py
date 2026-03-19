import threading 

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
        self._readers -= 1

        if self._readers > 0:
            return

        # Notify waiting writers that the _readers count is 0.
