import src.classes.kv_store as kv_store
import os
import time
import tempfile
import shutil
import threading
import src.config as config

def do_benchmark_funct(store, n, funct_type):
    start = time.perf_counter()
    ops = {
        "writes": lambda i: store.set(f"test_key_{i}", f"test_value_{i}"),
        "reads": lambda i: store.get(f"test_key_{i}"),
        "misses": lambda i: store.get(f"missing_key_{i}")
    }
    op = ops[funct_type]

    for i in range(0, n):
        op(i)

    end = time.perf_counter()
    
    return end - start 

def benchmark_reads(store, n):
    return do_benchmark_funct(store, n, "reads")

def benchmark_misses(store, n):
    return do_benchmark_funct(store, n, "misses")

def benchmark_writes(store, n):
    return do_benchmark_funct(store, n, "writes")

def benchmark_concurrent_reads(store, n, num_threads):
    per_thread = n // num_threads
    threads = []

    for t in range(num_threads):
        start_i = t * per_thread
        end_i = start_i + per_thread
        thread = threading.Thread(target=lambda s=start_i, e=end_i: [store.get(f"test_key_{i}") for i in range(s, e)])
        threads.append(thread)

    start = time.perf_counter()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return time.perf_counter() - start

def setup():
    pth = tempfile.mkdtemp()
    os.chdir(pth) 
    store = kv_store.KVStore()

    return store, pth 

def main():
    original_dir = os.getcwd()
    store, pth = setup() 
    print(f"Doing the benchmarks with N={config.BENCHMARK_N}...")
    total_write_time = benchmark_writes(store, config.BENCHMARK_N)
    print(f"Writes: {config.BENCHMARK_N} ops in {total_write_time:.2f}s -> {int(config.BENCHMARK_N / total_write_time)} ops/sec")
    total_read_time = benchmark_reads(store, config.BENCHMARK_N)
    print(f"Reads (1 thread):  {config.BENCHMARK_N} ops in {total_read_time:.2f}s -> {int(config.BENCHMARK_N / total_read_time)} ops/sec")
    num_threads = 4
    concurrent_read_time = benchmark_concurrent_reads(store, config.BENCHMARK_N, num_threads)
    print(f"Reads ({num_threads} threads): {config.BENCHMARK_N} ops in {concurrent_read_time:.2f}s -> {int(config.BENCHMARK_N / concurrent_read_time)} ops/sec")
    total_misses_time = benchmark_misses(store, config.BENCHMARK_N)
    print(f"Misses: {config.BENCHMARK_N} ops in {total_misses_time:.2f}s -> {int(config.BENCHMARK_N / total_misses_time)} ops/sec")
    os.chdir(original_dir)
    store._wal.close()
    shutil.rmtree(pth)

if __name__ == "__main__": 
    main() 