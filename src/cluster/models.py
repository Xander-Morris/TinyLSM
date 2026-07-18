"""Pydantic request models for TinyLSM's HTTP cluster API."""

from pydantic import BaseModel

class SetRequest(BaseModel):
    """Payload for a client write that assigns a value to a key."""
    key: str
    value: str

class DeleteRequest(BaseModel):
    """Payload for a client request that removes a key."""
    key: str

class ReplicateRequest(BaseModel):
    """Payload sent by a leader when it replicates one log entry."""
    operation: str
    key: str
    value: str = None
    index: int

class VoteRequest(BaseModel):
    """Payload used for pre-vote and vote requests during an election."""
    candidate_url: str
    term: int

class HeartbeatRequest(BaseModel):
    """Payload carrying leader identity, term, and follower catch-up entries."""
    leader_url: str
    term: int
    entries: list = []

class NodeRequest(BaseModel):
    """Payload for a cluster membership change."""
    node_url: str
