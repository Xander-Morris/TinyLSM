import config
from classes import kv_store 

def process_line(store, replay=False):
    line = line.strip() 
    sp = line.split(" ")
    operation = sp[0]
    
    if operation == "SET":
        if len(sp) < 3:
            print("3 arguments are required for SET!")
        else:
            if replay:
                store._set(sp[1], sp[2], True)
            else:
                store.set(sp[1], sp[2])
    elif operation == "DELETE":
        if len(sp) < 2:
            print("2 arguments required for delete")
        else:
            if replay:
                store._delete(sp[1], True)
            else:
                store.delete(sp[1])
    elif operation == "GET":
        if len(sp) < 2:
            print("2 arguments required for delete")
        else:
            return store.get(sp[1], True)
    else:
        print(f"Invalid operation: {operation}!")

def main():
    store = kv_store.KVStore(config.LOG_FILE_NAME, config.MAX_ENTRIES, config.MAX_SSTABLES)

    try:
        with open(config.LOG_FILE_NAME, 'r') as file:
            for line in file: 
                process_line(line, True)
        
        while True:
            command = input("Enter a command: ")

            if "EXIT" in command:
                break

            result = process_line(command)

            if result is not None:
                print(result)

    except:
        print("No file exists!")

if __name__ == "__main__":
    main()