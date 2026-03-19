from conftest import force_flush
import threading 

def test_concurrent_reads_consistency(store):
    n = 4

    for i in range(n):
        store.set(f"test_{i}", f"test_value_{i}")
    
    force_flush(store)
    results = []
    lock = threading.Lock()

    def worker(start, end):
        for i in range(start, end):
            value = store.get(f"test_{i}")

            with lock:
                results.append((f"test_{i}", value))

    threads = []

    for i in range(n):
        thread = threading.Thread(target=worker, args=(i, i + 1))
        threads.append(thread)
        thread.start() 

    for thread in threads:
        thread.join() 
    
    assert len(results) == n

    for i in range(n):
        assert store.get(f"test_{i}") == f"test_value_{i}"