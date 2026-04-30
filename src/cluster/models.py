from pydantic import BaseModel

class SetRequest(BaseModel):
    key: str
    value: str

class DeleteRequest(BaseModel):
    key: str

class ReplicateRequest(BaseModel):
    operation: str
    key: str
    value: str = None
    index: int

class VoteRequest(BaseModel):
    candidate_url: str
    term: int

class HeartbeatRequest(BaseModel):
    leader_url: str
    term: int
    entries: list = []

class NodeRequest(BaseModel):
    node_url: str