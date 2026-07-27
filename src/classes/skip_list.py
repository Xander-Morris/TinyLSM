"""
    A skip list class that allows searching for a specific key AND sequence value, not just the key.
"""

import random
from src.utils.versioning import pick_version

class Node: 
    def __init__(self, key, versions, level):
        self.key = key 
        self.versions = versions 
        self.forward = [None] * (level + 1)

class SkipList:
    def __init__(self, max_level=16, p=0.5):
        self._max_level = max_level
        self._p = p 
        self._level = 0 
        self._head = Node(None, [(None, None)], self._max_level) 
        self._all_keys = set()
        self._update_buf = [None] * (self._max_level + 1)

    def _random_level(self):
        lvl = 0
        while random.random() < self._p and lvl < self._max_level:
            lvl += 1
        return lvl

    def get_all_keys(self) -> set:
        return self._all_keys

    def get_all_entries_dict(self):
        """
            Returns dictionary with key -> versions mappings.
        """
        entries = {}
        current = self._head.forward[0]

        while current:
            entries[current.key] = current.versions
            current = current.forward[0]

        return entries

    def get_range_iter(self, start=None, end=None):
        current = self._head

        if start is not None:
            for i in range(self._level, -1, -1):
                while current.forward[i] and current.forward[i].key < start:
                    current = current.forward[i]
            current = current.forward[0]
        else:
            current = current.forward[0]

        while current and (end is None or current.key <= end):
            for seq, value in current.versions:
                yield (current.key, seq, value)
            current = current.forward[0]

    def get_all_entries_iter(self):
        current = self._head.forward[0]

        while current:
            for seq, value in current.versions:
                yield (current.key, seq, value)
            current = current.forward[0]

    def insert(self, key, seq, val):
        self._all_keys.add(key)
        update = self._update_buf
        current = self._head

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current and current.key == key:
            current.versions.append((seq, val))
            return 

        new_level = self._random_level()
        if new_level > self._level:
            for i in range(self._level + 1, new_level + 1):
                update[i] = self._head
            self._level = new_level

        new_node = Node(key, [(seq, val)], new_level)
        for i in range(new_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

    def search(self, key, at=None):
        current = self._head

        for i in range(self._level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]

        if not current or current.key != key:
            return

        return pick_version(current.versions, at)