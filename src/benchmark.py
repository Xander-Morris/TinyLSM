"""A small, repeatable benchmark for TinyLSM's local storage engine."""

import src.classes.kv_store as kv_store
import os
import time
import tempfile
import shutil
import threading
import src.config as config

BENCHMARK_DEFAULTS = {
    "MAX_MEMTABLE_SIZE": 1024 * 1024,
    "MAX_L0_FILES": 8,
    "WAL_BUFFER_SIZE": 1000,
}

def configure_benchmark_defaults():
    """Use benchmark-friendly settings unless the caller opts out explicitly."""
    if os.getenv("BENCHMARK_USE_STORE_CONFIG"):
        return

    config.MAX_MEMTABLE_SIZE = int(os.getenv("BENCHMARK_MAX_MEMTABLE_SIZE", BENCHMARK_DEFAULTS["MAX_MEMTABLE_SIZE"]))
    config.MAX_L0_FILES = int(os.getenv("BENCHMARK_MAX_L0_FILES", BENCHMARK_DEFAULTS["MAX_L0_FILES"]))
    config.WAL_BUFFER_SIZE = int(os.getenv("BENCHMARK_WAL_BUFFER_SIZE", BENCHMARK_DEFAULTS["WAL_BUFFER_SIZE"]))

def do_benchmark_funct(store, n, funct_type):
    """Run one named workload ``n`` times and return its elapsed seconds."""
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
    """Measure sequential point reads for keys written by the benchmark."""
    return do_benchmark_funct(store, n, "reads")

def benchmark_misses(store, n):
    """Measure point reads for keys that are absent from the store."""
    return do_benchmark_funct(store, n, "misses")

def benchmark_writes(store, n):
    """Measure sequential writes of unique benchmark keys."""
    return do_benchmark_funct(store, n, "writes")

def benchmark_concurrent_reads(store, n, num_threads):
    """Measure point reads split evenly across ``num_threads`` workers."""
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
    """Create an isolated temporary store for one benchmark run."""
    pth = tempfile.mkdtemp()
    os.chdir(pth) 
    store = kv_store.KVStore()

    return store, pth 

def main():
    """Run and print write, read, concurrent-read, and miss benchmarks."""
    original_dir = os.getcwd()
    configure_benchmark_defaults()
    store, pth = setup()
    try:
        print(
            f"Doing the benchmarks with N={config.BENCHMARK_N}, "
            f"MAX_MEMTABLE_SIZE={config.MAX_MEMTABLE_SIZE}, "
            f"MAX_L0_FILES={config.MAX_L0_FILES}, "
            f"WAL_BUFFER_SIZE={config.WAL_BUFFER_SIZE}..."
        )
        total_write_time = benchmark_writes(store, config.BENCHMARK_N)
        print(f"Writes: {config.BENCHMARK_N} ops in {total_write_time:.2f}s -> {int(config.BENCHMARK_N / total_write_time)} ops/sec")
        total_read_time = benchmark_reads(store, config.BENCHMARK_N)
        print(f"Reads (1 thread):  {config.BENCHMARK_N} ops in {total_read_time:.2f}s -> {int(config.BENCHMARK_N / total_read_time)} ops/sec")
        num_threads = 4
        concurrent_read_time = benchmark_concurrent_reads(store, config.BENCHMARK_N, num_threads)
        print(f"Reads ({num_threads} threads): {config.BENCHMARK_N} ops in {concurrent_read_time:.2f}s -> {int(config.BENCHMARK_N / concurrent_read_time)} ops/sec")
        total_misses_time = benchmark_misses(store, config.BENCHMARK_N)
        print(f"Misses: {config.BENCHMARK_N} ops in {total_misses_time:.2f}s -> {int(config.BENCHMARK_N / total_misses_time)} ops/sec")
    finally:
        store.close()
        os.chdir(original_dir)
        shutil.rmtree(pth)

if __name__ == "__main__": 
    main() 
