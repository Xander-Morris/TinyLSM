import os
import dotenv 

dotenv.load_dotenv()

LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "log_file.txt")
MAX_ENTRIES = int(os.getenv("MAX_ENTRIES", 10))
MAX_SSTABLES = int(os.getenv("MAX_SSTABLES", 20))
TOMBSTONE_VALUE = os.getenv("TOMBSTONE_VALUE", "__TOMBSTONE__")
HASH_FUNCTIONS = int(os.getenv("HASH_FUNCTIONS", 5))
BLOOM_FILTER_SIZE = int(os.getenv("BLOOM_FILTER_SIZE", 5))
SPARSE_INDEX_N = int(os.getenv("SPARSE_INDEX_N", 4))