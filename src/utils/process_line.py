"""Command parsing for TinyLSM's intentionally small interactive REPL."""

import inspect

_commands = {
    "set": {
        "syntax": "SET key value",
        "exec": lambda store, key, value: store.set(key, value),
    },
    "scan": {
        "syntax": "SCAN start end",
        "exec": lambda store, start, end: store.scan(start, end),
    },
    "delete": {
        "syntax": "DELETE key",
        "exec": lambda store, key: store.delete(key),
    },
    "get": {
        "syntax": "GET key",
        "exec": lambda store, key: store.get(key),
    },
    "stats": {
        "syntax": "STATS",
        "exec": lambda store: store.stats(),
    },
    "exit": {
        "syntax": "EXIT",
    }
}

def list_all_commands():
    print("List of commands: ")
    print("-----------------")
    for command, dict in _commands.items():
        print(dict.get("syntax", command))

def parse_line(line):
    """
        Parse line into tuple containing (command, unpacked_args).
    """
    line = line.strip()
    if not line:
        return ("", "")
    
    line = line.lower()

    try:
        args = line.split(" ")
    except ValueError as e:
        print(f"Parse error: {e}")
        return ("", "")

    command = args[0]
    unpacked_args = tuple(args[1:])
    
    return (command, unpacked_args)

def execute(store, command, unpacked_args):
    if command not in _commands or "exec" not in _commands[command]: 
        print(f"Error: {command} is not an executable command!")
        return None 
    
    func = _commands[command]["exec"]
    sig = inspect.signature(func) # ex: for set, it would be: (store, key, value)
    expected_count = len(sig.parameters) - 1 # minus 1 for the store argument 
    
    if len(unpacked_args) != expected_count:
        syntax = _commands[command].get("syntax", command)
        print(f"Error: Expected {expected_count} arguments, got {len(unpacked_args)}. Usage: {syntax}")
        return None
        
    return func(store, *unpacked_args)