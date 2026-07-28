"""Interactive command-line entry point for a local TinyLSM store."""

from src.classes import kv_store, repl_commands

def main():
    """Run the REPL until the user exits or interrupts it."""
    store = kv_store.KVStore()
    repl_handler = repl_commands.REPLCommandHandler(store)
    repl_handler.list_all_commands()

    try:
        while True:
            command = input("Enter a command: ")
            command, unpacked_args = repl_handler.parse_line(command)

            if command == "exit":
                break 

            result = repl_handler.execute(command, unpacked_args)

            if result is not None:
                print(result)
    except KeyboardInterrupt:
        pass
    finally:
        store.close()

if __name__ == "__main__":
    main()
