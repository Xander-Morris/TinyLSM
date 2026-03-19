def test_concurrent_reads_consistency(store):
    n = 4

    for i in range(n):
        store.set(f"test_{i}", f"test_{i} value")
    
    res = []
    