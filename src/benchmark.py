import src.classes.kv_store as kv_store 
import os 
import time 
import tempfile 
import shutil 
import src.config as config 

def do_benchmark_funct(store, n, funct_type):
    start = time.perf_counter()

    for i in range(0, n):
        if funct_type == "writes":
            store.set(f"test_key_{i}", f"test_value_{i}")
        elif funct_type == "reads":
            store.get(f"test_key_{i}")
        elif funct_type == "misses":
            store.get(f"missing_key_{i}")

    end = time.perf_counter()
    
    return end - start 

def benchmark_reads(store, n):
    return do_benchmark_funct(store, n, "reads")

def benchmark_misses(store, n):
    return do_benchmark_funct(store, n, "misses")

def benchmark_writes(store, n):
    return do_benchmark_funct(store, n, "writes")

def setup():
    pth = tempfile.mkdtemp()
    os.chdir(pth) 
    store = kv_store.KVStore()

    return store, pth 

def main():
    original_dir = os.getcwd()
    store, pth = setup() 
    total_write_time = benchmark_writes(store, config.BENCHMARK_N)
    print(f"Writes: {config.BENCHMARK_N} ops in {total_write_time:.2f}s -> {int(config.BENCHMARK_N / total_write_time)}")
    total_read_time = benchmark_reads(store, config.BENCHMARK_N)
    print(f"Reads: {config.BENCHMARK_N} ops in {total_read_time:.2f}s -> {int(config.BENCHMARK_N / total_read_time)}")
    total_misses_time = benchmark_misses(store, config.BENCHMARK_N)
    print(f"Misses: {config.BENCHMARK_N} ops in {total_misses_time:.2f}s -> {int(config.BENCHMARK_N / total_misses_time)}")
    os.chdir(original_dir)
    shutil.rmtree(pth)

if __name__ == "__main__": 
    main() 