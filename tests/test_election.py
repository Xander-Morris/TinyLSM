import subprocess
import requests
import sys
import time
from utils import wait_for


def _kill_port(port):
    result = subprocess.run(
        f'netstat -ano | findstr ":{port}" | findstr "LISTENING"',
        shell=True, capture_output=True, text=True,
    )
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if parts:
            subprocess.run(f"taskkill /F /PID {parts[-1]}", shell=True, capture_output=True)
    time.sleep(0.3)


def start_node(port, data_dir, leader_url, all_nodes):
    return subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), leader_url, all_nodes],
        stdout=subprocess.DEVNULL,
        stderr=None,
    )

def test_election_after_leader_failure(tmp_path_factory):
    ports = [8300, 8301, 8302]
    leader_url = f"http://localhost:{ports[0]}"
    all_nodes = ",".join(f"http://localhost:{p}" for p in ports)
    procs = {}

    for port in ports:
        _kill_port(port)

    for port in ports:
        data_dir = tmp_path_factory.mktemp(f"election_{port}")
        procs[port] = start_node(port, data_dir, leader_url, all_nodes)

    for port in ports:
        ok = wait_for(lambda p=port: requests.get(f"http://localhost:{p}/get", params={"key": "__health__"}, timeout=2).status_code == 200, timeout=15.0)
        assert ok, f"Node on port {port} did not start in time"

    # Confirm cluster is working.
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "before", "value": "1"}, timeout=5)

    # Kill the leader.
    procs[ports[0]].terminate()
    procs[ports[0]].wait()

    survivors = [ports[1], ports[2]]

    # Wait for the survivors to elect a new leader.
    def new_leader_elected():
        statuses = [requests.get(f"http://localhost:{p}/status", timeout=2).json() for p in survivors]
        leaders = [s["leader"] for s in statuses]
        terms = [s["term"] for s in statuses]
        # Both survivors agree on the same leader, it is not the dead node, and term > 0.
        return leaders[0] == leaders[1] and leaders[0] != leader_url and terms[0] > 0

    try:
        assert wait_for(new_leader_elected, timeout=5.0)

        # Writes to the surviving cluster should succeed.
        new_leader = requests.get(f"http://localhost:{ports[1]}/status", timeout=2).json()["leader"]
        resp = requests.post(f"{new_leader}/set", json={"key": "after", "value": "2"}, timeout=5)
        assert resp.json().get("ok") is True

        # Both survivors should have the new write.
        wait_for(lambda: all(
            requests.get(f"http://localhost:{p}/get", params={"key": "after"}, timeout=2).json()["value"] == "2"
            for p in survivors
        ))
        for port in survivors:
            assert requests.get(f"http://localhost:{port}/get", params={"key": "after"}, timeout=2).json()["value"] == "2"
    finally:
        for port in survivors:
            procs[port].terminate()
            procs[port].wait()

def test_vote_granted_once_per_term(tmp_path_factory):
    port = 8310
    _kill_port(port)
    url = f"http://localhost:{port}"
    data_dir = tmp_path_factory.mktemp("vote_once")
    # A node that is its own leader never runs its own election timer, so its
    # term only moves when these direct /vote calls move it.
    proc = start_node(port, data_dir, url, url)

    try:
        ok = wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200, timeout=15.0)
        assert ok, f"Node on port {port} did not start in time"

        term = 1
        candidate_a = "http://localhost:9001"
        candidate_b = "http://localhost:9002"

        # First candidate to ask for this term gets the vote.
        first = requests.post(f"{url}/vote", json={"candidate_url": candidate_a, "term": term}, timeout=2)
        assert first.json()["vote_granted"] is True

        # A second candidate racing for the same term must be refused, or the cluster could split.
        second = requests.post(f"{url}/vote", json={"candidate_url": candidate_b, "term": term}, timeout=2)
        assert second.json()["vote_granted"] is False

        # Once the term advances, voting reopens.
        later = requests.post(f"{url}/vote", json={"candidate_url": candidate_b, "term": term + 1}, timeout=2)
        assert later.json()["vote_granted"] is True
    finally:
        proc.terminate()
        proc.wait()
