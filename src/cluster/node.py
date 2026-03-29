import random
import sys
import os
import json
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import requests
from typing import Literal
import time 
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

REPLICATION_LOG_FILE = "replication.log"

app = FastAPI()
LEADER = None
NODES = []
store = None
port = None
my_url = None
log = []  # List containing elements with {"index": int, "operation": str, "key": str, "value": str}.
log_index = 0
term = 0
voted_for = None 
last_heartbeat = time.time() 

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
                    log.append(entry)
                    log_index = entry["index"]
    except FileNotFoundError:
        pass

def _start_election():
    global term, voted_for
    term += 1
    voted_for = my_url 
    votes = 1 

    for node_url in NODES:
        if node_url != my_url:
            try:
                response = requests.post(f"{node_url}/vote", json={"candidate_url": my_url, "term": term}, timeout=1)
            
                if response.json().get("vote_granted"):
                    votes += 1
            except Exception:
                pass

    total_nodes = len(NODES)
    majority = (total_nodes // 2) + 1

    if votes >= majority:
        global LEADER
        LEADER = my_url

class SetRequest(BaseModel):
    key: str
    value: str

class DeleteRequest(BaseModel):
    key: str

class ReplicateRequest(BaseModel):
    operation: str # "set" or "delete"
    key: str 
    value: str = None 

class VoteRequest(BaseModel):
    candidate_url: str
    term: int 

def do_replicated_operation(operation: Literal["set", "delete"], key: str, value: str | None = None):
    if operation != "set" and operation != "delete":
        return {"ok": False}
    
    json_tbl = {"key": key}
    if operation == "set":
        json_tbl["value"] = value

    if my_url != LEADER:
        # Forward it to the leader if this node is not the leader.
        response = requests.post(f"{LEADER}/{operation}", json=json_tbl)
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

    successes = 1  # The leader itself counts as 1.
    total_nodes = len(NODES)
    majority = (total_nodes // 2) + 1

    for node_url in NODES:
        if node_url != my_url:
            try:
                requests.post(f"{node_url}/replicate", json={"operation": operation, **json_tbl}, timeout=1)
                successes += 1
            except Exception:
                pass

    if successes >= majority:
        return {"ok": True}
    else:
        return {"ok": False, "error": "failed to reach majority"}

@app.get("/get")
def get(key: str):
    value = store.get(key)
    return {"key": key, "value": value}

@app.get("/sync")
def sync(from_index: int):
    return {"entries": log[from_index:]}

@app.post("/set")
def set(req: SetRequest):
    return do_replicated_operation("set", req.key, req.value)

@app.post("/delete")
def delete(req: DeleteRequest):
    return do_replicated_operation("delete", req.key)

@app.post("/heartbeat")
def heartbeat():
    global last_heartbeat 
    last_heartbeat = time.time()

    return {"ok": True}

@app.post("/replicate")
def replicate(req: ReplicateRequest):
    if req.operation == "set":
        store.set(req.key, req.value)
    elif req.operation == "delete":
        store.delete(req.key)

    return {"ok": True}

@app.post("/vote")
def vote(req: VoteRequest):
    if req.term > term or (req.term == term and (voted_for is None or voted_for == req.candidate_url)):
        global term, voted_for, last_heartbeat
        term = req.term 
        voted_for = req.candidate_url 
        last_heartbeat = time.time() 

        return {"vote_granted": True} 

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

    if my_url == LEADER:
        _load_log_from_disk()
        
        def _send_heartbeats():
            while True:
                for node_url in NODES:
                    if node_url != my_url:
                        try:
                            requests.post(f"{node_url}/heartbeat", timeout=0.5)
                        except Exception:
                            pass
                time.sleep(0.15)

        threading.Thread(target=_send_heartbeats, daemon=True).start()
    else:
        try:
            response = requests.get(f"{LEADER}/sync", params={"from_index": 0})

            for entry in response.json()["entries"]:
                if entry["operation"] == "set":
                    store.set(entry["key"], entry["value"])
                elif entry["operation"] == "delete":
                    store.delete(entry["key"])
        except Exception:
            pass

        ELECTION_TIMEOUT = random.uniform(0.3, 0.6)

        def _election_timeout_watcher():
            while True: 
                if time.time() - last_heartbeat > ELECTION_TIMEOUT: 
                    _start_election() 
                time.sleep(0.05)
        
        threading.Thread(target=_election_timeout_watcher, daemon=True).start()

    uvicorn.run(app, host="0.0.0.0", port=port)