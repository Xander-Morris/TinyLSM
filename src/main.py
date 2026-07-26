"""Interactive command-line entry point for a local TinyLSM store."""

from src.utils.process_line import parse_line, execute, list_all_commands
from src.classes import kv_store

def main():
    """Run the REPL until the user exits or interrupts it."""
    store = kv_store.KVStore()
    list_all_commands()

    try:
        while True:
            command = input("Enter a command: ")
            command, unpacked_args = parse_line(command)

            if command == "exit":
                break 

            result = execute(store, command, unpacked_args)

            if result is not None:
                print(result)
    except KeyboardInterrupt:
        pass
    finally:
        store.close()

if __name__ == "__main__":
    main()
