import os
import dotenv 

dotenv.load_dotenv()

LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "log_file.txt")
MAX_MEMTABLE_SIZE = int(os.getenv("MAX_MEMTABLE_SIZE", 4096)) # Default of 4KB
TOMBSTONE_VALUE = os.getenv("TOMBSTONE_VALUE", "__TOMBSTONE__")
HASH_FUNCTIONS = int(os.getenv("HASH_FUNCTIONS", 5))
BLOOM_FILTER_SIZE = int(os.getenv("BLOOM_FILTER_SIZE", 5))
SPARSE_INDEX_N = int(os.getenv("SPARSE_INDEX_N", 4))
MAX_L0_FILES = int(os.getenv("MAX_L0_FILES", 2))
BENCHMARK_N = int(os.getenv("BENCHMARK_N", 10000))
WAL_BUFFER_SIZE = int(os.getenv("WAL_BUFFER_SIZE", 100))