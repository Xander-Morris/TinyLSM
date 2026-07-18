"""HTTP routes exposed by a TinyLSM cluster node."""

import json
import time
import requests
from fastapi import APIRouter
import src.cluster.context as ctx
from src.cluster import models

router = APIRouter()

@router.get("/get")
def get(key: str, consistent: bool = False):
    """Read a key locally or forward a requested consistent read to the leader."""
    with ctx.state:
        leader = ctx.state.leader

    if consistent and ctx.my_url != leader:
        return ctx._try_operation_until_success_or_max_tries(
            lambda: requests.get(f"{leader}/get", params={"key": key, "consistent": True}).json(),
            max_tries=3,
        )

    value = ctx.store.get(key)
    return {"key": key, "value": value}

@router.get("/sync")
def sync(from_index: int):
    """Return log entries, and a snapshot when the caller is too far behind."""
    with ctx.state:
        current_snapshot_index = ctx.state.snapshot_index
        log_copy = list(ctx.state.log)

    if from_index < current_snapshot_index:
        with open(ctx.SNAPSHOT_FILE) as f:
            snapshot = json.loads(f.read())
        entries = [e for e in log_copy if e["index"] > current_snapshot_index]
        return {"snapshot": snapshot, "entries": entries}
    else:
        return {"entries": [e for e in log_copy if e["index"] > from_index]}

@router.post("/set")
def set(req: models.SetRequest):
    """Replicate a client key/value write through the current leader."""
    return ctx.do_replicated_operation("set", req.key, req.value)

@router.post("/delete")
def delete(req: models.DeleteRequest):
    """Replicate a client delete through the current leader."""
    return ctx.do_replicated_operation("delete", req.key)

@router.post("/add_node")
def add_node(req: models.NodeRequest):
    """Replicate the addition of a node URL to cluster membership."""
    return ctx.do_replicated_operation("add_node", req.node_url)

@router.post("/remove_node")
def remove_node(req: models.NodeRequest):
    """Replicate the removal of a node URL from cluster membership."""
    return ctx.do_replicated_operation("remove_node", req.node_url)

@router.post("/heartbeat")
def heartbeat(req: models.HeartbeatRequest):
    """Accept a leader heartbeat and apply any newly supplied entries."""
    with ctx.state:
        current_term = ctx.state.term

    if req.term < current_term:
        with ctx.state:
            return {"ok": True, "log_index": ctx.state.log_index}

    new_entries, log_index = ctx._update_state_from_heartbeat(req)

    for entry in new_entries:
        ctx._handle_operation(entry["operation"], entry["key"], entry["value"])
        ctx._append_log_entry(entry)

    return {"ok": True, "log_index": log_index}

@router.post("/replicate")
def replicate(req: models.ReplicateRequest):
    """Apply and persist one entry sent directly by the leader."""
    ctx._handle_operation(req.operation, req.key, req.value)

    entry = {"index": req.index, "operation": req.operation, "key": req.key, "value": req.value}
    with ctx.state:
        ctx.state.log_index = req.index
        ctx.state.log.append(entry)

    ctx._append_log_entry(entry)
    return {"ok": True}

@router.get("/status")
def status():
    """Report the node's current leader, term, and own URL."""
    with ctx.state:
        return {"leader": ctx.state.leader, "term": ctx.state.term, "my_url": ctx.my_url}

@router.post("/vote")
def vote(req: models.VoteRequest):
    """Grant at most one vote per eligible term and persist that decision."""
    vote_granted = False
    save_data = None

    with ctx.state:
        if req.term > ctx.state.term or (req.term == ctx.state.term and (ctx.state.voted_for is None or ctx.state.voted_for == req.candidate_url)):
            ctx.state.term = req.term
            ctx.state.voted_for = req.candidate_url
            ctx.state.last_heartbeat = time.time()
            vote_granted = True
            save_data = (ctx.state.term, ctx.state.voted_for)

    if save_data:
        ctx._persist_vote_state(*save_data)

    return {"vote_granted": vote_granted}

@router.post("/prevote")
def prevote(req: models.VoteRequest):
    """Report whether this node currently appears ready for an election."""
    with ctx.state:
        elapsed = time.time() - ctx.state.last_heartbeat
        timeout = ctx.state.election_timeout
    return {"vote_granted": elapsed > timeout * 0.5}
