"""Interactive command-line entry point for a local TinyLSM store."""

from src.utils.process_line import process_line
from src.classes import kv_store

def main():
    """Run the REPL until the user exits or interrupts it."""
    store = kv_store.KVStore()

    try:
        while True:
            command = input("Enter a command: ")

            if command.strip() == "EXIT":
                break

            result = process_line(store, command)

            if result is not None:
                print(result)
    except KeyboardInterrupt:
        pass
    finally:
        store.close()

if __name__ == "__main__":
    main()
