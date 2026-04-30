import random
import sys
import os
import json
import uvicorn
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
import requests
from typing import Literal
import time
from src import config as config
from src.classes import raft_state as raft_state

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

REPLICATION_LOG_FILE = "replication.log"
STATE_FILE = "state.json"
SNAPSHOT_FILE = "snapshot.json"

state = raft_state.RaftState()
store = None
port = None
my_url = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    with state:
        state.last_heartbeat = time.time()
    yield

app = FastAPI(lifespan=lifespan)

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

def _send_heartbeats():
    def _heartbeat_one(node_url):
        try:
            with state:
                follower_index = state.follower_indices.get(node_url, 0)
                entries_to_send = list(state.log[follower_index:])
                current_term = state.term

            response = requests.post(f"{node_url}/heartbeat", json={
                "leader_url": my_url,
                "term": current_term,
                "entries": entries_to_send,
            }, timeout=0.1)

            with state:
                state.follower_indices[node_url] = response.json().get("log_index", follower_index)
        except Exception:
            pass

    while True:
        with state:
            should_continue = state.leader == my_url and state.voted_for == my_url
            nodes_copy = list(state.nodes)

        if not should_continue:
            break

        threads = [
            threading.Thread(target=_heartbeat_one, args=(url,), daemon=True)
            for url in nodes_copy if url != my_url
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(0.15)


def _start_election():
    def _send_vote_requests_to_all_other_nodes(vote_term, prevote=False):
        votes = 1
        votes_lock = threading.Lock()

        with state:
            nodes_copy = list(state.nodes)
            majority = (len(state.nodes) // 2) + 1

        def _request_vote(node_url):
            nonlocal votes
            try:
                endpoint = "/prevote" if prevote else "/vote"
                response = requests.post(f"{node_url}{endpoint}", json={"candidate_url": my_url, "term": vote_term}, timeout=0.2)
                if response.json().get("vote_granted"):
                    with votes_lock:
                        votes += 1
            except Exception:
                pass

        threads = [threading.Thread(target=_request_vote, args=(url,)) for url in nodes_copy if url != my_url]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return votes >= majority

    with state:
        pre_term = state.term

    if not _send_vote_requests_to_all_other_nodes(pre_term + 1, prevote=True):
        return

    with state:
        if state.term != pre_term:
            return
        state.term += 1
        state.voted_for = my_url
        my_term = state.term
        save_term, save_voted = state.term, state.voted_for

    _persist_vote_state(save_term, save_voted)

    if _send_vote_requests_to_all_other_nodes(my_term):
        with state:
            if state.term == my_term:
                state.leader = my_url
        threading.Thread(target=_send_heartbeats, daemon=True).start()


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

def _do_compaction(current_index):
    snapshot_data = store.dump()
    _write_snapshot(current_index, snapshot_data)
    with state:
        state.log.clear()
    with open(REPLICATION_LOG_FILE, 'w') as f:
        f.write("")

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

    _handle_operation(operation, key, value)

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
            requests.post(f"{node_url}/replicate", json={"operation": operation, "index": current_index, **json_tbl}, timeout=1)
            with successes_lock:
                successes += 1
        except Exception:
            pass

    threads = [
        threading.Thread(target=_replicate_one, args=(url,))
        for url in nodes_copy if url != my_url
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if successes >= majority:
        return {"ok": True}
    else:
        return {"ok": False, "error": "failed to reach majority"}

def _update_state_from_heartbeat(req):
    new_entries = []
    log_index = -1 

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

    return (new_entries, log_index)

@app.get("/get")
def get(key: str, consistent: bool = False):
    with state:
        leader = state.leader

    if consistent and my_url != leader:
        return _try_operation_until_success_or_max_tries(
            lambda: requests.get(f"{leader}/get", params={"key": key, "consistent": True}).json(),
            max_tries=3,
        )

    value = store.get(key)
    return {"key": key, "value": value}


@app.get("/sync")
def sync(from_index: int):
    with state:
        current_snapshot_index = state.snapshot_index
        log_copy = list(state.log)

    if from_index < current_snapshot_index:
        with open(SNAPSHOT_FILE) as f:
            snapshot = json.loads(f.read())
        entries = [e for e in log_copy if e["index"] > current_snapshot_index]
        return {"snapshot": snapshot, "entries": entries}
    else:
        return {"entries": [e for e in log_copy if e["index"] > from_index]}


@app.post("/set")
def set(req: SetRequest):
    return do_replicated_operation("set", req.key, req.value)


@app.post("/delete")
def delete(req: DeleteRequest):
    return do_replicated_operation("delete", req.key)


@app.post("/add_node")
def add_node(req: NodeRequest):
    return do_replicated_operation("add_node", req.node_url)

@app.post("/remove_node")
def remove_node(req: NodeRequest):
    return do_replicated_operation("remove_node", req.node_url)

@app.post("/heartbeat")
def heartbeat(req: HeartbeatRequest):
    with state:
        current_term = state.term

    if req.term < current_term:
        with state:
            return {"ok": True, "log_index": state.log_index}

    new_entries, log_index = _update_state_from_heartbeat(req)

    for entry in new_entries:
        if entry["operation"] == "set":
            store.set(entry["key"], entry["value"])
        elif entry["operation"] == "delete":
            store.delete(entry["key"])
        _append_log_entry(entry)

    return {"ok": True, "log_index": log_index}

@app.post("/replicate")
def replicate(req: ReplicateRequest):
    _handle_operation(req.operation, req.key, req.value)

    entry = {"index": req.index, "operation": req.operation, "key": req.key, "value": req.value}
    with state:
        state.log_index = req.index
        state.log.append(entry)

    _append_log_entry(entry)
    return {"ok": True}

@app.get("/status")
def status():
    with state:
        return {"leader": state.leader, "term": state.term, "my_url": my_url}

@app.post("/vote")
def vote(req: VoteRequest):
    vote_granted = False
    save_data = None

    with state:
        if req.term > state.term or (req.term == state.term and (state.voted_for is None or state.voted_for == req.candidate_url)):
            state.term = req.term
            state.voted_for = req.candidate_url
            state.last_heartbeat = time.time()
            vote_granted = True
            save_data = (state.term, state.voted_for)

    if save_data:
        _persist_vote_state(*save_data)

    return {"vote_granted": vote_granted}


@app.post("/prevote")
def prevote(req: VoteRequest):
    with state:
        elapsed = time.time() - state.last_heartbeat
        timeout = state.election_timeout
    return {"vote_granted": elapsed > timeout * 0.5}

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    data_dir = sys.argv[2] if len(sys.argv) > 2 else f"node_data_{port}"
    leader = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8000"
    nodes = sys.argv[4].split(",") if len(sys.argv) > 4 else [
        "http://localhost:8000",
        "http://localhost:8001",
        "http://localhost:8002",
    ]

    state.leader = leader
    state.nodes = nodes
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(data_dir)
    store = kv_store.KVStore()
    my_url = os.getenv("MY_URL", f"http://localhost:{port}")

    _load_snapshot_from_disk()
    _load_log_from_disk()
    _load_state_from_disk()

    if my_url != state.leader:
        try:
            response = _try_operation_until_success_or_max_tries(
                lambda: requests.get(f"{state.leader}/sync", params={"from_index": state.log_index}).json(),
                max_tries=5,
                delay=0.5,
            )

            def _replay(entries_list):
                for entry in entries_list:
                    if entry["operation"] == "set":
                        store.set(entry["key"], entry["value"])
                    elif entry["operation"] == "delete":
                        store.delete(entry["key"])
                    state.log.append(entry)
                    state.log_index = entry["index"]

            if "snapshot" in response:
                for key, value in response["snapshot"]["data"].items():
                    store.set(key, value)
                state.log_index = response["snapshot"]["index"]

            _replay(response["entries"])
        except Exception:
            pass

    state.election_timeout = random.uniform(0.5, 1.5)
    state.last_heartbeat = time.time()

    def _election_timeout_watcher():
        while True:
            with state:
                leader = state.leader
                elapsed = time.time() - state.last_heartbeat
                timeout = state.election_timeout

            if leader != my_url and elapsed > timeout:
                _start_election()
                with state:
                    state.election_timeout = random.uniform(0.5, 1.5)
                    state.last_heartbeat = time.time()

            time.sleep(0.05)

    if my_url == state.leader:
        state.voted_for = my_url
        threading.Thread(target=_send_heartbeats, daemon=True).start()

    threading.Thread(target=_election_timeout_watcher, daemon=True).start()

    uvicorn.run(app, host="0.0.0.0", port=port)
