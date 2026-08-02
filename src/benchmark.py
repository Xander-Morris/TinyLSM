"""A small, repeatable benchmark for TinyLSM's local storage engine."""

import src.classes.kv_store as kv_store
import os
import time
import tempfile
import shutil
import statistics
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

def _percentiles_ms(latencies):
    """Return (p50, p99) latency in milliseconds for a list of per-op seconds."""
    quantiles = statistics.quantiles(latencies, n=100, method="inclusive")
    return quantiles[49] * 1000, quantiles[98] * 1000

def do_benchmark_funct(store, n, funct_type):
    """Run one named workload ``n`` times and return (elapsed seconds, per-op latencies)."""
    ops = {
        "writes": lambda i: store.set(f"test_key_{i}", f"test_value_{i}"),
        "reads": lambda i: store.get(f"test_key_{i}"),
        "misses": lambda i: store.get(f"missing_key_{i}")
    }
    op = ops[funct_type]
    latencies = [0.0] * n

    start = time.perf_counter()
    for i in range(0, n):
        op_start = time.perf_counter()
        op(i)
        latencies[i] = time.perf_counter() - op_start
    end = time.perf_counter()

    return end - start, latencies

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
    """Measure point reads split evenly across ``num_threads`` workers, returning (elapsed seconds, per-op latencies)."""
    per_thread = n // num_threads
    latencies = [0.0] * (per_thread * num_threads)

    def _worker(start_i, end_i):
        for i in range(start_i, end_i):
            op_start = time.perf_counter()
            store.get(f"test_key_{i}")
            latencies[i] = time.perf_counter() - op_start

    threads = [
        threading.Thread(target=_worker, args=(t * per_thread, t * per_thread + per_thread))
        for t in range(num_threads)
    ]

    start = time.perf_counter()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    return time.perf_counter() - start, latencies

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
        def _report(label, n, total_time, latencies):
            p50, p99 = _percentiles_ms(latencies)
            print(f"{label}: {n} ops in {total_time:.2f}s -> {int(n / total_time)} ops/sec (p50={p50:.3f}ms, p99={p99:.3f}ms)")

        total_write_time, write_latencies = benchmark_writes(store, config.BENCHMARK_N)
        _report("Writes", config.BENCHMARK_N, total_write_time, write_latencies)

        total_read_time, read_latencies = benchmark_reads(store, config.BENCHMARK_N)
        _report("Reads (1 thread) ", config.BENCHMARK_N, total_read_time, read_latencies)

        num_threads = 4
        concurrent_read_time, concurrent_latencies = benchmark_concurrent_reads(store, config.BENCHMARK_N, num_threads)
        _report(f"Reads ({num_threads} threads)", len(concurrent_latencies), concurrent_read_time, concurrent_latencies)

        total_misses_time, miss_latencies = benchmark_misses(store, config.BENCHMARK_N)
        _report("Misses", config.BENCHMARK_N, total_misses_time, miss_latencies)
    finally:
        store.close()
        os.chdir(original_dir)
        shutil.rmtree(pth)

if __name__ == "__main__": 
    main() 
