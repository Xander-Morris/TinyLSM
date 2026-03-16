import config
from classes import kv_store 

def main():
    store = kv_store.KVStore()

    with open(config.LOG_FILE_NAME, 'r') as file:
        for line in file: 
            line = line.strip() 
            sp = line.split(" ")
            operation = sp[0]
            
            if operation == "SET":
                if len(sp) < 3:
                    print("3 arguments are required for SET!")
                else:
                    store.set(sp[1], sp[2])
            elif operation == "DELETE":
                if len(sp) < 2:
                    print("2 arguments required for delete")
                else:
                    store.delete(sp[1])
            else:
                print(f"Invalid operation: {operation}!")

if __name__ == "__main__":
    main()