import src.utils as utils 
from classes import kv_store 

def main():
    store = kv_store.KVStore()

    try: 
        while True:
            command = input("Enter a command: ")

            if "EXIT" in command:
                break

            result = utils.process_line(store, command)

            if result is not None:
                print(result)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()