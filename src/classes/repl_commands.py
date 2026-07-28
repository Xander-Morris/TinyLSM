from dataclasses import dataclass
import inspect
from typing import Callable, Optional

@dataclass
class REPLCommand:
    syntax: str
    exec: Optional[Callable] = None

class REPLCommandHandler:
    def __init__(self, store):
        self.store = store
        self.commands = {
            "set": REPLCommand("SET key value", self.cmd_set),
            "scan": REPLCommand("SCAN start end", self.cmd_scan),
            "delete": REPLCommand("DELETE key", self.cmd_delete),
            "get": REPLCommand("GET key at (Note that at is optional)", self.cmd_get),
            "stats": REPLCommand("STATS", self.cmd_stats),
            "exit": REPLCommand("EXIT"),
        }

    def execute(self, command, unpacked_args):
        if command not in self.commands or not self.commands[command].exec: 
            print(f"Error: {command} is not an executable command!")
            return None 
        
        func = self.commands[command].exec

        if not func:
            return 
            
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        
        min_args = sum(1 for p in params if p.default == inspect.Parameter.empty)
        max_args = len(params)
        
        provided_count = len(unpacked_args)
        if provided_count < min_args or provided_count > max_args:
            print(f"Error: {command.upper()} expects between {min_args} and {max_args} arguments. You provided {provided_count}.")
            print(f"Syntax: {self.commands[command].syntax}")
            return None

        try:
            return func(*unpacked_args)
        except ValueError as e:
            print(f"Error: Invalid argument type. {e}")
            return None
        except Exception as e:
            print(f"Runtime Error: {e}")
            return None

    def parse_line(self, line):
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

    def list_all_commands(self):
        print("List of commands: ")
        print("-----------------")
        for command_name, command in self.commands.items():
            print(command.syntax)

    def cmd_set(self, key: str, value: str):
        return self.store.set(key, value)

    def cmd_scan(self, start: str, end: str):
        return self.store.scan(start, end)

    def cmd_delete(self, key: str):
        return self.store.delete(key)

    def cmd_get(self, key: str, at: Optional[str] = None):
        parsed_at = None if at is None else int(at)
        return self.store.get(key, parsed_at)

    def cmd_stats(self):
        return self.store.stats()