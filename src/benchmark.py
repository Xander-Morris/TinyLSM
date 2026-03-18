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

    return store 

def main():
    store = setup() 
    print(benchmark_writes(store, config.BENCHMARK_N))
    print(benchmark_reads(store, config.BENCHMARK_N))
    print(benchmark_misses(store, config.BENCHMARK_N))
    shutil.rmtree(os.getcwd())

if __name__ == "__main__": 
    main() 