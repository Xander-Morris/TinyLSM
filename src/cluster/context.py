"""Shared coordination and persistence helpers for a cluster node.

This module intentionally implements a compact, Raft-inspired learning
protocol rather than the complete Raft specification.  The running node sets
``store`` and ``my_url`` during startup; route handlers then use this module's
shared state to coordinate requests.

Log entries move through two stages.  An entry is first *appended* to a node's
log, and only once the leader knows a majority holds it is it *committed* and
then *applied* to the key/value store.  Followers learn the leader's commit
index from heartbeats, so no node ever applies a write that could still be
lost in a leader crash.
"""

import json
import os
import threading
import time
import requests
from typing import Literal
from src import config
from src.classes import raft_state

REPLICATION_LOG_FILE = "replication.log"
STATE_FILE = "state.json"
SNAPSHOT_FILE = "snapshot.json"
COMMIT_WAIT_SECONDS = 1.0

state = raft_state.RaftState()
store = None
my_url = None

# Serializes appliers so committed entries reach the store exactly in log order.
_apply_lock = threading.Lock()

def _try_operation_until_success_or_max_tries(operation, max_tries, delay=0.1):
    """Retry a network operation, returning its first success or re-raising."""
    tries = 0
    while tries < max_tries:
        tries += 1
        try:
            return operation()
        except Exception as e:
            print(f"Attempt {tries} failed: {e}")
            if tries == max_tries:
                raise
            time.sleep(delay)

def _write_snapshot(index, term, snapshot_data):
    """Atomically write a point-in-time key/value snapshot to disk."""
    with open("snapshot.tmp", 'w') as file:
        file.write(json.dumps({"index": index, "term": term, "data": snapshot_data}))
    # This is atomic on both Windows and Linux, so it can never be in a partial state, which would cause corruption.
    os.replace("snapshot.tmp", SNAPSHOT_FILE)

def _load_snapshot_from_disk():
    """Restore the locally persisted cluster snapshot when one is available."""
    try:
        with open(SNAPSHOT_FILE, 'r') as f:
            saved = json.loads(f.read())
            for key, value in saved["data"].items():
                if store:
                    store.set(key, value)
            state.log_index = saved["index"]
            state.snapshot_index = state.log_index
            state.snapshot_term = saved.get("term", 0)
            # A snapshot only ever holds applied, and therefore committed, state.
            state.commit_index = state.last_applied = state.log_index
    except FileNotFoundError:
        pass

def _load_state_from_disk():
    """Restore the durable election term and vote for this node."""
    try:
        with open(STATE_FILE, 'r') as f:
            saved = json.loads(f.read())
            state.term = saved["term"]
            state.voted_for = saved["voted_for"]
    except FileNotFoundError:
        pass

def _persist_vote_state(term, voted_for):
    """Atomically persist the election state that Raft requires to survive."""
    with open("state.tmp", 'w') as f:
        f.write(json.dumps({"term": term, "voted_for": voted_for}))
    os.replace("state.tmp", STATE_FILE)

def _append_log_entry(entry):
    """Append one replication entry to the local durable operation log."""
    with open(REPLICATION_LOG_FILE, 'a') as f:
        f.write(json.dumps(entry) + '\n')

def _rewrite_log_file(entries):
    """Atomically replace the durable operation log with ``entries``."""
    with open("replication.tmp", 'w') as f:
        for entry in entries:
            f.write(json.dumps(entry) + '\n')
    os.replace("replication.tmp", REPLICATION_LOG_FILE)

def _load_log_from_disk():
    """Load log entries newer than the snapshot into in-memory cluster state.

    Loaded entries are not applied here.  The node does not yet know which of
    them committed, so it waits to hear a commit index from the leader.
    """
    try:
        with open(REPLICATION_LOG_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    entry = json.loads(line)
                    if entry["index"] == state.log_index + 1:
                        state.log.append(entry)
                        state.log_index = entry["index"]
    except FileNotFoundError:
        pass

def _last_log_position():
    """Return ``(term, index)`` of this node's newest log entry.  Caller holds ``state``."""
    if state.log:
        return state.log[-1].get("term", 0), state.log_index
    return state.snapshot_term, state.log_index

def _candidate_log_is_current(last_log_term, last_log_index):
    """Return whether a candidate's log is at least as up to date as ours.

    This is Raft's election restriction.  Comparing the last entry's term first,
    then its index, guarantees any winner already holds every committed entry,
    because a committed entry lives on a majority and every winning candidate
    needs a vote from at least one member of that majority.  Caller holds ``state``.
    """
    return (last_log_term, last_log_index) >= _last_log_position()

def _handle_operation(operation, key, value):
    """Apply one replicated storage or membership operation locally.

    Every operation is idempotent so a restarted node can safely re-apply
    committed entries it may already have written to its store.
    """
    if operation == "set":
        if store:
            store.set(key, value)
    elif operation == "delete":
        if store:
            store.delete(key)
    elif operation == "add_node":
        with state:
            if key not in state.nodes:
                state.nodes.append(key)
    elif operation == "remove_node":
        with state:
            if key in state.nodes:
                state.nodes.remove(key)

def _append_entries(entries):
    """Append entries that extend the local log without a gap, then return ``log_index``.

    Entries at or below ``log_index`` are already held, and an entry past the
    next slot is refused so the log never has holes.  The leader resends
    anything refused on its next heartbeat.  Nothing is applied here.
    """
    with state:
        for entry in sorted(entries, key=lambda e: e["index"]):
            if entry["index"] == state.log_index + 1:
                state.log.append(entry)
                state.log_index = entry["index"]
                _append_log_entry(entry)
        return state.log_index

def _apply_committed():
    """Apply every committed entry that has not reached the store yet, in log order."""
    with _apply_lock:
        with state:
            pending = [e for e in state.log if state.last_applied < e["index"] <= state.commit_index]

        for entry in pending:
            _handle_operation(entry["operation"], entry["key"], entry["value"])
            with state:
                state.last_applied = entry["index"]

def _advance_commit_index():
    """On the leader, commit the highest index that a majority of nodes hold."""
    with state:
        if state.leader != my_url:
            return
        match_indices = [state.log_index] + [state.follower_indices.get(url, 0) for url in state.nodes if url != my_url]
        majority = (len(state.nodes) // 2) + 1
        match_indices.sort(reverse=True)
        # A follower can report a longer log than ours if it kept stale entries
        # from an old leader, so never commit past what the leader itself holds.
        majority_index = min(match_indices[min(majority, len(match_indices)) - 1], state.log_index)
        if majority_index > state.commit_index:
            state.commit_index = majority_index

    _apply_committed()

def _do_compaction():
    """Snapshot the applied state and drop the log entries that snapshot covers.

    Only applied entries are truncated, so entries still waiting on a majority
    survive compaction.
    """
    with _apply_lock:
        with state:
            applied = state.last_applied
            covered = [e for e in state.log if e["index"] <= applied]
        if not covered or not store:
            return

        snapshot_term = covered[-1].get("term", 0)
        _write_snapshot(applied, snapshot_term, store.dump())

        with state:
            state.log = [e for e in state.log if e["index"] > applied]
            state.snapshot_index = applied
            state.snapshot_term = snapshot_term
            _rewrite_log_file(state.log)

def _update_state_from_heartbeat(req):
    """Record a leader heartbeat, append its entries, and adopt its commit index."""
    with state:
        state.last_heartbeat = time.time()
        state.leader = req.leader_url
        state.term = req.term

    log_index = _append_entries(req.entries)

    with state:
        # Never commit past what this node actually holds.
        state.commit_index = max(state.commit_index, min(req.commit_index, log_index))

    return log_index

def do_replicated_operation(operation: Literal["set", "delete", "add_node", "remove_node"], key: str, value: str | None = None):
    """Forward or replicate a client mutation, requiring majority acknowledgement."""
    if operation not in ("set", "delete", "add_node", "remove_node"):
        return {"ok": False}

    json_tbl: dict[str, str | None] = {"key": key}
    if operation == "set":
        json_tbl["value"] = value

    with state:
        leader = state.leader

    if my_url != leader:
        response = _try_operation_until_success_or_max_tries(
            lambda: requests.post(f"{leader}/{operation}", json=json_tbl, timeout=5),
            max_tries=3,
        )
        if response:
            return response.json()

    with state:
        state.log_index += 1
        entry = {"index": state.log_index, "term": state.term, "operation": operation, "key": key, "value": value}
        state.log.append(entry)
        _append_log_entry(entry)
        should_compact = len(state.log) > config.LOG_COMPACTION_THRESHOLD
        current_index = state.log_index
        nodes_copy = list(state.nodes)

    def _replicate_one(node_url):
        """Send the pending entry to one follower and record how far its log reaches."""
        try:
            res = requests.post(f"{node_url}/replicate", json={"operation": operation, "index": current_index, "term": entry["term"], **json_tbl}, timeout=1)

            if res and res.ok:
                with state:
                    state.follower_indices[node_url] = res.json().get("log_index", 0)
        except Exception:
            pass

    threads = [threading.Thread(target=_replicate_one, args=(url,)) for url in nodes_copy if url != my_url]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    _advance_commit_index()

    # A follower that refused the entry because an earlier one was still in
    # flight picks it up on the next heartbeat, which can commit it for us.
    deadline = time.time() + COMMIT_WAIT_SECONDS
    while True:
        with state:
            committed = state.commit_index >= current_index
        if committed or time.time() >= deadline:
            break
        time.sleep(0.02)

    if committed:
        if should_compact:
            _do_compaction()
        return {"ok": True}
    else:
        return {"ok": False, "error": "failed to reach majority"}
