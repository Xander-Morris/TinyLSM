"""Shared mutable state for TinyLSM's educational replication layer."""

import threading
import dataclasses

@dataclasses.dataclass
class RaftState:
    """A lockable container for one node's term, membership, and log state."""
    term: int = 0
    voted_for: str | None = None
    leader: str | None = None
    nodes: list = dataclasses.field(default_factory=list)
    log: list = dataclasses.field(default_factory=list)
    log_index: int = 0
    snapshot_index: int = 0
    follower_indices: dict = dataclasses.field(default_factory=dict)
    last_heartbeat: float = 0.0
    election_timeout: float = 1.0
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock, repr=False)

    def __enter__(self):
        """Acquire the state lock for a short, coordinated update."""
        self._lock.acquire()
        return self

    def __exit__(self, *args):
        """Release the state lock acquired by :meth:`__enter__`."""
        self._lock.release()
