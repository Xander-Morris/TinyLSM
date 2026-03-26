import sys
import os
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import requests 
from typing import Literal

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

app = FastAPI()
LEADER = None
NODES = []
store = None 
port = None
my_url = None
log = []  # List containing elements with {"index": int, "operation": str, "key": str, "value": str}.
log_index = 0

class SetRequest(BaseModel):
    key: str
    value: str

class DeleteRequest(BaseModel):
    key: str

class ReplicateRequest(BaseModel):
    operation: str # "set" or "delete"
    key: str 
    value: str = None 

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
    log.append({"index": log_index, "operation": operation, "key": key, "value": value})

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

@app.post("/replicate")
def replicate(req: ReplicateRequest):
    if req.operation == "set":
        store.set(req.key, req.value)
    elif req.operation == "delete":
        store.delete(req.key)

    return {"ok": True}

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

    if my_url != LEADER:
        try:
            response = requests.get(f"{LEADER}/sync", params={"from_index": 0})

            for entry in response.json()["entries"]:
                if entry["operation"] == "set":
                    store.set(entry["key"], entry["value"])
                elif entry["operation"] == "delete":
                    store.delete(entry["key"])
        except Exception:
            pass

    uvicorn.run(app, host="0.0.0.0", port=port)