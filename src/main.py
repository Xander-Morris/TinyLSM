import src.utils as utils
from src.classes import kv_store

def main():
    store = kv_store.KVStore()

    try:
        while True:
            command = input("Enter a command: ")

            if command.strip() == "EXIT":
                break

            result = utils.process_line(store, command)

            if result is not None:
                print(result)
    except KeyboardInterrupt:
        pass
    finally:
        store.close()

if __name__ == "__main__":
    main()