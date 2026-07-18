"""FastAPI application startup and background loops for one TinyLSM node."""

import random
import sys
import os
import uvicorn
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import requests
import time
import src.cluster.context as ctx
from src.cluster.routes import router
import src.classes.kv_store as kv_store

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize the heartbeat timestamp for FastAPI's application lifespan."""
    with ctx.state:
        ctx.state.last_heartbeat = time.time()
    yield

app = FastAPI(lifespan=lifespan)
app.include_router(router)

def _send_heartbeats():
    """Continuously send the leader's log tail to every reachable follower."""
    def _heartbeat_one(node_url):
        """Send the follower only the log suffix it has not acknowledged."""
        try:
            with ctx.state:
                follower_index = ctx.state.follower_indices.get(node_url, 0)
                entries_to_send = list(ctx.state.log[follower_index:])
                current_term = ctx.state.term

            response = requests.post(f"{node_url}/heartbeat", json={
                "leader_url": ctx.my_url,
                "term": current_term,
                "entries": entries_to_send,
            }, timeout=0.1)

            with ctx.state:
                ctx.state.follower_indices[node_url] = response.json().get("log_index", follower_index)
        except Exception:
            pass

    while True:
        with ctx.state:
            should_continue = ctx.state.leader == ctx.my_url and ctx.state.voted_for == ctx.my_url
            nodes_copy = list(ctx.state.nodes)

        if not should_continue:
            break

        threads = [
            threading.Thread(target=_heartbeat_one, args=(url,), daemon=True)
            for url in nodes_copy if url != ctx.my_url
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(0.15)


def _start_election():
    """Run an election for local leadership after a heartbeat timeout.

    The previous pre-vote gate could livelock two followers: the first follower
    reset after the second denied an early pre-vote, then denied the second
    follower for the same reason.  Starting the normal, term-based election
    directly lets the randomized election deadlines break that cycle.
    """
    def _send_vote_requests_to_all_other_nodes(vote_term, prevote=False):
        """Collect enough pre-votes or votes to form the current majority."""
        votes = 1
        votes_lock = threading.Lock()

        with ctx.state:
            nodes_copy = list(ctx.state.nodes)
            majority = (len(ctx.state.nodes) // 2) + 1

        def _request_vote(node_url):
            """Ask one peer to support this candidate for ``vote_term``."""
            nonlocal votes
            try:
                endpoint = "/prevote" if prevote else "/vote"
                response = requests.post(f"{node_url}{endpoint}", json={"candidate_url": ctx.my_url, "term": vote_term}, timeout=0.2)
                if response.json().get("vote_granted"):
                    with votes_lock:
                        votes += 1
            except Exception:
                pass

        threads = [threading.Thread(target=_request_vote, args=(url,)) for url in nodes_copy if url != ctx.my_url]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return votes >= majority

    with ctx.state:
        if ctx.state.leader == ctx.my_url:
            return
        ctx.state.term += 1
        ctx.state.voted_for = ctx.my_url
        my_term = ctx.state.term
        save_term, save_voted = ctx.state.term, ctx.state.voted_for

    ctx._persist_vote_state(save_term, save_voted)

    if _send_vote_requests_to_all_other_nodes(my_term):
        with ctx.state:
            if ctx.state.term == my_term:
                ctx.state.leader = ctx.my_url
                ctx.state.last_heartbeat = time.time()
        threading.Thread(target=_send_heartbeats, daemon=True).start()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    data_dir = sys.argv[2] if len(sys.argv) > 2 else f"node_data_{port}"
    leader = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8000"
    nodes = sys.argv[4].split(",") if len(sys.argv) > 4 else [
        "http://localhost:8000",
        "http://localhost:8001",
        "http://localhost:8002",
    ]

    ctx.state.leader = leader
    ctx.state.nodes = nodes
    os.makedirs(data_dir, exist_ok=True)
    os.chdir(data_dir)
    ctx.store = kv_store.KVStore()
    ctx.my_url = os.getenv("MY_URL", f"http://localhost:{port}")

    ctx._load_snapshot_from_disk()
    ctx._load_log_from_disk()
    ctx._load_state_from_disk()

    if ctx.my_url != ctx.state.leader:
        try:
            response = ctx._try_operation_until_success_or_max_tries(
                lambda: requests.get(f"{ctx.state.leader}/sync", params={"from_index": ctx.state.log_index}).json(),
                max_tries=5,
                delay=0.5,
            )

            def _replay(entries_list):
                """Apply synchronized entries in log order during node startup."""
                for entry in entries_list:
                    ctx._handle_operation(entry["operation"], entry["key"], entry["value"])
                    ctx.state.log.append(entry)
                    ctx.state.log_index = entry["index"]

            if response and "snapshot" in response:
                for key, value in response["snapshot"]["data"].items():
                    ctx.store.set(key, value)
                ctx.state.log_index = response["snapshot"]["index"]

            if response:
                _replay(response["entries"])
        except Exception:
            pass

    ctx.state.election_timeout = random.uniform(0.5, 1.5)
    ctx.state.last_heartbeat = time.time()

    def _election_timeout_watcher():
        """Start an election when this follower has missed its leader's heartbeat."""
        while True:
            with ctx.state:
                leader = ctx.state.leader
                elapsed = time.time() - ctx.state.last_heartbeat
                timeout = ctx.state.election_timeout

            if leader != ctx.my_url and elapsed > timeout:
                _start_election()
                with ctx.state:
                    ctx.state.election_timeout = random.uniform(0.5, 1.5)
                    ctx.state.last_heartbeat = time.time()

            time.sleep(0.05)

    if ctx.my_url == ctx.state.leader:
        ctx.state.voted_for = ctx.my_url
        threading.Thread(target=_send_heartbeats, daemon=True).start()

    threading.Thread(target=_election_timeout_watcher, daemon=True).start()

    uvicorn.run(app, host="0.0.0.0", port=port)
