import sys
import os
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
import src.classes.kv_store as kv_store

app = FastAPI()
store = None 

class SetRequest(BaseModel):
    key: str
    value: str

class DeleteRequest(BaseModel):
    key: str

class ReplicateRequest(BaseModel):
    operation: str # "set" or "delete"
    key: str 
    value: str = None 

@app.get("/get")
def get(key: str):
    value = store.get(key)
    return {"key": key, "value": value}

@app.post("/set")
def set(req: SetRequest):
    store.set(req.key, req.value)
    return {"ok": True}

@app.post("/delete")
def delete(req: DeleteRequest):
    store.delete(req.key)
    return {"ok": True}

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