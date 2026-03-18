def process_line(store, line, replay=False):
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
    elif operation == "SCAN":
        if len(sp) < 3:
            print("3 arguments are required for SCAN!")
        else:
            tuples = store.scan(sp[1], sp[2])

            for key, value in tuples: 
                print(f"Key: {key}, value: {value}")
    elif operation == "DELETE":
        if len(sp) < 2:
            print("2 arguments are required for DELETE!")
        else:
            if replay:
                store._delete(sp[1], True)
            else:
                store.delete(sp[1])
    elif operation == "GET":
        if len(sp) < 2:
            print("2 arguments are required for GET!")
        else:
            return f"Key {sp[1]} has value of {store.get(sp[1])}"
    else:
        print(f"Invalid operation: {operation}!")