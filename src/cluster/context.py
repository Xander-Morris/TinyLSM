import json
import threading
import time
import requests
from typing import Literal
from src import config
from src.classes import raft_state

REPLICATION_LOG_FILE = "replication.log"
STATE_FILE = "state.json"
SNAPSHOT_FILE = "snapshot.json"

state = raft_state.RaftState()
store = None
my_url = None

def _try_operation_until_success_or_max_tries(operation, max_tries, delay=0.1):
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

def _write_snapshot(index, snapshot_data):
    with open(SNAPSHOT_FILE, 'w') as f:
        f.write(json.dumps({"index": index, "data": snapshot_data}))
    with state:
        state.snapshot_index = index

def _load_snapshot_from_disk():
    try:
        with open(SNAPSHOT_FILE, 'r') as f:
            saved = json.loads(f.read())
            for key, value in saved["data"].items():
                store.set(key, value)
            state.log_index = saved["index"]
            state.snapshot_index = state.log_index
    except FileNotFoundError:
        pass

def _load_state_from_disk():
    try:
        with open(STATE_FILE, 'r') as f:
            saved = json.loads(f.read())
            state.term = saved["term"]
            state.voted_for = saved["voted_for"]
    except FileNotFoundError:
        pass

def _persist_vote_state(term, voted_for):
    with open(STATE_FILE, 'w') as f:
        f.write(json.dumps({"term": term, "voted_for": voted_for}))

def _append_log_entry(entry):
    with open(REPLICATION_LOG_FILE, 'a') as f:
        f.write(json.dumps(entry) + '\n')

def _load_log_from_disk():
    try:
        with open(REPLICATION_LOG_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    entry = json.loads(line)
                    if entry["index"] > state.log_index:
                        state.log.append(entry)
                        state.log_index = entry["index"]
    except FileNotFoundError:
        pass

def _handle_operation(operation, key, value):
    if operation == "set":
        store.set(key, value)
    elif operation == "delete":
        store.delete(key)
    elif operation == "add_node":
        with state:
            state.nodes.append(key)
    elif operation == "remove_node":
        with state:
            state.nodes.remove(key)

def _do_compaction(current_index):
    snapshot_data = store.dump()
    _write_snapshot(current_index, snapshot_data)
    with state:
        state.log.clear()
    with open(REPLICATION_LOG_FILE, 'w') as f:
        f.write("")

def _update_state_from_heartbeat(req):
    new_entries = []
    with state:
        state.last_heartbeat = time.time()
        state.leader = req.leader_url
        state.term = req.term
        for entry in req.entries:
            if entry["index"] > state.log_index:
                state.log.append(entry)
                state.log_index = entry["index"]
                new_entries.append(entry)
        log_index = state.log_index
    return new_entries, log_index

def do_replicated_operation(operation: Literal["set", "delete", "add_node", "remove_node"], key: str, value: str | None = None):
    if operation not in ("set", "delete", "add_node", "remove_node"):
        return {"ok": False}

    json_tbl = {"key": key}
    if operation == "set":
        json_tbl["value"] = value

    with state:
        leader = state.leader

    if my_url != leader:
        response = _try_operation_until_success_or_max_tries(
            lambda: requests.post(f"{leader}/{operation}", json=json_tbl, timeout=5),
            max_tries=3,
        )
        return response.json()

    with state:
        state.log_index += 1
        entry = {"index": state.log_index, "operation": operation, "key": key, "value": value}
        state.log.append(entry)
        should_compact = len(state.log) > config.LOG_COMPACTION_THRESHOLD
        current_index = state.log_index

    _append_log_entry(entry)

    if should_compact:
        _do_compaction(current_index)

    successes = 1
    successes_lock = threading.Lock()

    with state:
        nodes_copy = list(state.nodes)
        total_nodes = len(state.nodes)

    majority = (total_nodes // 2) + 1

    def _replicate_one(node_url):
        nonlocal successes
        try:
            res = requests.post(f"{node_url}/replicate", json={"operation": operation, "index": current_index, **json_tbl}, timeout=1)
            
            if res and res.ok:
                with successes_lock:
                    successes += 1
        except Exception:
            pass

    threads = [threading.Thread(target=_replicate_one, args=(url,)) for url in nodes_copy if url != my_url]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if successes >= majority:
        _handle_operation(operation, key, value) # only apply operation if majority consensus is reached
        return {"ok": True}
    else:
        return {"ok": False, "error": "failed to reach majority"}