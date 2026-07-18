"""The singleton marker used internally to represent a deleted key."""


class TombstoneType:
    """A distinct sentinel so deleted values cannot be confused with strings."""

    _instance = None

    def __new__(cls):
        """Return the one shared tombstone instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        """Show a useful representation in debugging output."""
        return "<TOMBSTONE>"
