"""Command parsing for TinyLSM's intentionally small interactive REPL."""

import shlex

def process_line(store, line):
    """Execute one REPL command and return output that the caller should print.

    Quoted tokens are handled by :mod:`shlex`; commands that perform their own
    output, such as ``SCAN``, return ``None``.
    """
    line = line.strip()
    if not line:
        return

    try:
        sp = shlex.split(line)
    except ValueError as e:
        print(f"Parse error: {e}")
        return

    if not sp:
        return

    operation = sp[0]

    if operation == "SET":
        if len(sp) < 3:
            print("3 arguments are required for SET!")
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
            store.delete(sp[1])
    elif operation == "GET":
        if len(sp) < 2:
            print("2 arguments are required for GET!")
        else:
            return f"Key {sp[1]} has value of {store.get(sp[1])}"
    elif operation == "STATS":
        return store.stats()
    else:
        print(f"Invalid operation: {operation}!")
