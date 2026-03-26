import sys
import os
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import src.cluster.config as cluster_config 
import requests 
from typing import Literal

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

app = FastAPI()
store = None 
port = None

class SetRequest(BaseModel):
    key: str
    value: str

class DeleteRequest(BaseModel):
    key: str

class ReplicateRequest(BaseModel):
    operation: str # "set" or "delete"
    key: str 
    value: str = None 

def do_replicated_operation(operation: Literal["set", "delete"], key: str, value: str | None):
    if operation != "set" and operation != "delete":
        return {"ok": False}
    
    my_url = f"http://localhost:{port}"
    json_tbl = {"key": key}
    if operation == "set":
        json_tbl["value"] = value

    if my_url != cluster_config.LEADER:
        # Forward it to the leader if this node is not the leader.
        response = requests.post(f"{cluster_config.LEADER}/{operation}", json=json_tbl)
        return response.json()

    if operation == "set":
        store.set(key, value)
    elif operation == "delete":
        store.delete(key)

    for node_url in cluster_config.NODES:
        if node_url != my_url:
            requests.post(f"{node_url}/replicate", json={"operation": operation, **json_tbl})
    
    return {"ok": True}

@app.get("/get")
def get(key: str):
    value = store.get(key)
    return {"key": key, "value": value}

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
    data_dir = f"node_data_{port}"
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(data_dir)
    store = kv_store.KVStore()
    uvicorn.run(app, host="0.0.0.0", port=port)