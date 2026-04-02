import random
import sys
import os
import json
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
import requests
from typing import Literal
import time
import threading
from src import config as config 

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

REPLICATION_LOG_FILE = "replication.log"
STATE_FILE = "state.json"
SNAPSHOT_FILE = "snapshot.json"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global last_heartbeat
    last_heartbeat = time.time()
    yield

app = FastAPI(lifespan=lifespan)
LEADER = None
NODES = []
store = None
port = None
my_url = None
log = []  # List containing elements with {"index": int, "operation": str, "key": str, "value": str}.
log_index = 0
snapshot_index = 0
term = 0
voted_for = None
last_heartbeat = time.time()
follower_indices = {}  # {node_url: last_known_log_index}
_vote_lock = threading.Lock()

def _write_snapshot(index, data):
    global snapshot_index

    with open(SNAPSHOT_FILE, 'w') as f:
        f.write(json.dumps({"index": index, "data": data}))
        snapshot_index = index 

def _load_snapshot_from_disk():
    global log_index, snapshot_index

    try:
        with open(SNAPSHOT_FILE, 'r') as f: 
            state = json.loads(f.read())
            for key, value in state["data"].items(): 
                store.set(key, value)
            log_index = state["index"]
            snapshot_index = log_index 
    except FileNotFoundError:
        pass 

def _load_state_from_disk():
    global term, voted_for

    try:
        with open(STATE_FILE, 'r') as f:
            state = json.loads(f.read())
            term = state["term"]
            voted_for = state["voted_for"]
    except FileNotFoundError:
        pass

def _write_to_state():
    with open(STATE_FILE, 'w') as f:
        f.write(json.dumps({"term": term, "voted_for": voted_for}))

def _append_log_entry(entry):
    with open(REPLICATION_LOG_FILE, 'a') as f:
        f.write(json.dumps(entry) + '\n')

def _load_log_from_disk():
    global log, log_index
    try:
        with open(REPLICATION_LOG_FILE, 'r') as f:
            for line in f:
                line = line.strip()

                if line:
                    entry = json.loads(line)

                    if entry["index"] > log_index:
                        log.append(entry)
                        log_index = entry["index"]
    except FileNotFoundError:
        pass

def _send_heartbeats():
    def _heartbeat_one(node_url):
        try:
            follower_index = follower_indices.get(node_url, 0)
            entries_to_send = log[follower_index:]
            response = requests.post(f"{node_url}/heartbeat", json={
                "leader_url": my_url,
                "term": term,
                "entries": entries_to_send,
            }, timeout=0.1)
            follower_indices[node_url] = response.json().get("log_index", follower_index)
        except Exception:
            pass

    while LEADER == my_url and voted_for == my_url:
        threads = [
            threading.Thread(target=_heartbeat_one, args=(url,), daemon=True)
            for url in NODES if url != my_url
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(0.15)

def _start_election():
    global term, voted_for, LEADER

    def _send_vote_requests_to_all_other_nodes(vote_term, prevote=False):
        votes = 1
        vote_lock = threading.Lock()

        def _request_vote(node_url):
            nonlocal votes

            try:
                endpoint = "/prevote" if prevote else "/vote"
                response = requests.post(f"{node_url}{endpoint}", json={"candidate_url": my_url, "term": vote_term}, timeout=0.2)
                
                if response.json().get("vote_granted"):
                    with vote_lock:
                        votes += 1
            except Exception:
                pass

        threads = [threading.Thread(target=_request_vote, args=(url,)) for url in NODES if url != my_url]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return votes >= (len(NODES) // 2) + 1

    if not _send_vote_requests_to_all_other_nodes(term + 1, prevote=True):
        return

    with _vote_lock:
        term += 1
        voted_for = my_url
    my_term = term
    _write_to_state()

    if _send_vote_requests_to_all_other_nodes(my_term) and term == my_term:
        LEADER = my_url
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

def do_replicated_operation(operation: Literal["set", "delete"], key: str, value: str | None = None):
    if operation != "set" and operation != "delete":
        return {"ok": False}
    
    json_tbl = {"key": key}
    if operation == "set":
        json_tbl["value"] = value

    if my_url != LEADER:
        # Forward it to the leader if this node is not the leader.
        response = requests.post(f"{LEADER}/{operation}", json=json_tbl, timeout=5)
        return response.json()

    if operation == "set":
        store.set(key, value)
    elif operation == "delete":
        store.delete(key)

    global log_index
    log_index += 1
    entry = {"index": log_index, "operation": operation, "key": key, "value": value}
    log.append(entry)
    _append_log_entry(entry)

    if len(log) > config.LOG_COMPACTION_THRESHOLD:
        state = store.dump()
        _write_snapshot(log_index, state)
        log.clear()

        with open(REPLICATION_LOG_FILE, 'w') as f:
            f.write("")

    successes = 1  # The leader itself counts as 1.
    total_nodes = len(NODES)
    majority = (total_nodes // 2) + 1
    _successes_lock = threading.Lock()

    def _replicate_one(node_url):
        nonlocal successes
        try:
            requests.post(f"{node_url}/replicate", json={"operation": operation, "index": log_index, **json_tbl}, timeout=1)
            with _successes_lock:
                successes += 1
        except Exception:
            pass

    threads = [
        threading.Thread(target=_replicate_one, args=(url,))
        for url in NODES if url != my_url
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if successes >= majority:
        return {"ok": True}
    else:
        return {"ok": False, "error": "failed to reach majority"}

@app.get("/get")
def get(key: str, consistent: bool = False):
    if consistent and my_url != LEADER:
        return requests.get(f"{LEADER}/get", params={"key": key, "consistent": True}).json()

    value = store.get(key)
    return {"key": key, "value": value}

@app.get("/sync")
def sync(from_index: int):
    if from_index < snapshot_index:
        # The caller is behind the snapshot, so send the snapshot and the remaining log.
        snapshot = json.loads(open(SNAPSHOT_FILE).read())
        entries = [e for e in log if e["index"] > snapshot_index]

        return {"snapshot": snapshot, "entries": entries}
    else:
        return {"entries": [e for e in log if e["index"] > from_index]}

@app.post("/set")
def set(req: SetRequest):
    return do_replicated_operation("set", req.key, req.value)

@app.post("/delete")
def delete(req: DeleteRequest):
    return do_replicated_operation("delete", req.key)

@app.post("/heartbeat")
def heartbeat(req: HeartbeatRequest):
    global last_heartbeat, LEADER, term, log_index

    if req.term >= term:
        last_heartbeat = time.time()
        LEADER = req.leader_url
        term = req.term

        for entry in req.entries:
            if entry["index"] > log_index:
                if entry["operation"] == "set":
                    store.set(entry["key"], entry["value"])
                elif entry["operation"] == "delete":
                    store.delete(entry["key"])
                log.append(entry)
                log_index = entry["index"]
                _append_log_entry(entry)

    return {"ok": True, "log_index": log_index}

@app.post("/replicate")
def replicate(req: ReplicateRequest):
    global log_index

    if req.operation == "set":
        store.set(req.key, req.value)
    elif req.operation == "delete":
        store.delete(req.key)

    log_index = req.index
    entry = {"index": req.index, "operation": req.operation, "key": req.key, "value": req.value}
    log.append(entry)
    _append_log_entry(entry)

    return {"ok": True}

@app.get("/status")
def status():
    return {"leader": LEADER, "term": term, "my_url": my_url}

@app.post("/vote")
def vote(req: VoteRequest):
    global term, voted_for, last_heartbeat

    with _vote_lock:
        if req.term > term or (req.term == term and (voted_for is None or voted_for == req.candidate_url)):
            term = req.term
            voted_for = req.candidate_url
            last_heartbeat = time.time()
            _write_to_state()

            return {"vote_granted": True}
        
    return {"vote_granted": False}

@app.post("/prevote")
def prevote(req: VoteRequest):
    return {"vote_granted": time.time() - last_heartbeat > ELECTION_TIMEOUT * 0.5}

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    data_dir = sys.argv[2] if len(sys.argv) > 2 else f"node_data_{port}"
    leader = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8000"
    nodes = sys.argv[4].split(",") if len(sys.argv) > 4 else [
        "http://localhost:8000",
        "http://localhost:8001", 
        "http://localhost:8002",
    ]
    LEADER = leader
    NODES = nodes
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(data_dir)
    store = kv_store.KVStore()
    my_url = f"http://localhost:{port}"

    _load_snapshot_from_disk()
    _load_log_from_disk()
    _load_state_from_disk()

    # Sync from the configured leader if it's a new node joining an existing cluster.
    if my_url != LEADER:
        try:
            response = requests.get(f"{LEADER}/sync", params={"from_index": log_index})
            response = response.json()

            def _replay(entries_list):
                global log_index 

                for entry in entries_list:
                    if entry["operation"] == "set":
                        store.set(entry["key"], entry["value"])
                    elif entry["operation"] == "delete":
                        store.delete(entry["key"])
                    log.append(entry)
                    log_index = entry["index"]

            if "snapshot" in response:
                for key, value in response["snapshot"]["data"].items():
                    store.set(key, value)
                log_index = response["snapshot"]["index"]

            _replay(response["entries"])
        except Exception:
            pass

    ELECTION_TIMEOUT = random.uniform(0.5, 1.5)
    last_heartbeat = time.time() 

    def _election_timeout_watcher():
        global last_heartbeat, ELECTION_TIMEOUT

        while True:
            if my_url != LEADER and time.time() - last_heartbeat > ELECTION_TIMEOUT:
                _start_election()
                ELECTION_TIMEOUT = random.uniform(0.5, 1.5)
                last_heartbeat = time.time() 
            time.sleep(0.05)

    if my_url == LEADER:
        voted_for = my_url 
        threading.Thread(target=_send_heartbeats, daemon=True).start()

    threading.Thread(target=_election_timeout_watcher, daemon=True).start()

    uvicorn.run(app, host="0.0.0.0", port=port)