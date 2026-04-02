import pytest
import subprocess
import time
import requests
import sys
import os
import json 
from utils import wait_for

def _kill_port(port):
    """Kill any process listening on the given port (Windows)."""
    result = subprocess.run(
        f'netstat -ano | findstr ":{port}" | findstr "LISTENING"',
        shell=True, capture_output=True, text=True,
    )
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if parts:
            subprocess.run(f"taskkill /F /PID {parts[-1]}", shell=True, capture_output=True)
    time.sleep(0.3)

@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    procs = []
    ports = [8100, 8101, 8102]
    leader = f"http://localhost:{ports[0]}"
    nodes = ",".join(f"http://localhost:{p}" for p in ports)

    try:
        for port in ports:
            _kill_port(port)

        for port in ports:
            data_dir = tmp_path_factory.mktemp(f"node_{port}")
            proc = subprocess.Popen(
                [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), leader, nodes],
                stdout=subprocess.DEVNULL,
                stderr=None,
            )
            procs.append(proc)

        for port in ports:
            ok = wait_for(lambda p=port: requests.get(f"http://localhost:{p}/get", params={"key": "__health__"}, timeout=2).status_code == 200, timeout=15.0)
            assert ok, f"Node on port {port} did not start in time"

        def cluster_stable():
            statuses = [requests.get(f"http://localhost:{p}/status", timeout=2).json() for p in ports]
            leaders = [s["leader"] for s in statuses]
            return len(set(leaders)) == 1 and all(leaders)

        ok = wait_for(cluster_stable, timeout=10.0)
        if not ok:
            for p in ports:
                try:
                    s = requests.get(f"http://localhost:{p}/status", timeout=2).json()
                    print(f"  port {p}: {s}", flush=True)
                except Exception as e:
                    print(f"  port {p}: ERROR {e}", flush=True)
        assert ok, "Cluster did not elect a stable leader"
        yield ports
    finally:
        for proc in procs:
            proc.terminate()
            proc.wait()

def test_replication(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "foo", "value": "bar"}, timeout=5)
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "foo"}, timeout=2).json()["value"] == "bar"
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "foo"}, timeout=2).json()["value"] == "bar"

def test_follower_write_forwarded_to_leader(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[1]}/set", json={"key": "follower_write", "value": "yes"}, timeout=5)
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "follower_write"}, timeout=2).json()["value"] == "yes"
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "follower_write"}, timeout=2).json()["value"] == "yes"

def test_delete_replicates(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "to_delete", "value": "temp"}, timeout=5)
    requests.post(f"http://localhost:{ports[0]}/delete", json={"key": "to_delete"}, timeout=5)
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "to_delete"}, timeout=2).json()["value"] is None
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "to_delete"}, timeout=2).json()["value"] is None

def test_catchup_after_restart(cluster, tmp_path_factory):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "before_down", "value": "1"}, timeout=5)
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "before_down"}, timeout=2).json()["value"] == "1"
        for p in ports
    ))
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "missed_write", "value": "2"}, timeout=5)
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "missed_write"}, timeout=2).json()["value"] == "2"
        for p in ports
    ))
    port = 8103
    data_dir = tmp_path_factory.mktemp("node_8103")
    leader = f"http://localhost:{ports[0]}"
    nodes = ",".join(f"http://localhost:{p}" for p in ports + [port])
    proc = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), leader, nodes],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}, timeout=2).json()["value"] == "2", timeout=5.0)
    assert requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}, timeout=2).json()["value"] == "2"
    proc.terminate()
    proc.wait()

def test_replication_log_survives_leader_restart(tmp_path_factory):
    port = 8200
    url = f"http://localhost:{port}"
    data_dir = tmp_path_factory.mktemp("persist_leader")

    proc = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), url, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)
    requests.post(f"{url}/set", json={"key": "alpha", "value": "1"}, timeout=5)
    requests.post(f"{url}/set", json={"key": "beta", "value": "2"}, timeout=5)

    proc.terminate()
    proc.wait()

    proc2 = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), url, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)

    follower_port = 8201
    follower_url = f"http://localhost:{follower_port}"
    follower_dir = tmp_path_factory.mktemp("persist_follower")
    follower_nodes = f"{url},{follower_url}"

    follower_proc = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(follower_port), str(follower_dir), url, follower_nodes],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for(lambda: requests.get(f"{follower_url}/get", params={"key": "alpha"}, timeout=2).json()["value"] == "1", timeout=5.0)
        assert requests.get(f"{follower_url}/get", params={"key": "alpha"}, timeout=2).json()["value"] == "1"
        assert requests.get(f"{follower_url}/get", params={"key": "beta"}, timeout=2).json()["value"] == "2"
    finally:
        proc2.terminate()
        proc2.wait()
        follower_proc.terminate()
        follower_proc.wait()

def test_compaction(tmp_path_factory):
    port = 8400
    url = f"http://localhost:{port}"
    data_dir = tmp_path_factory.mktemp("compaction")

    proc = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), url, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)

    for i in range(1002):
        requests.post(f"{url}/set", json={"key": f"key_{i}", "value": str(i)}, timeout=5)

    proc.terminate()
    proc.wait()

    snapshot_path = data_dir / "snapshot.json"
    assert snapshot_path.exists()

    snapshot = json.loads(snapshot_path.read_text())
    assert snapshot["index"] == 1001
    assert snapshot["data"]["key_0"] == "0"
    assert snapshot["data"]["key_1000"] == "1000"

    proc2 = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), url, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)

    try:
        assert requests.get(f"{url}/get", params={"key": "key_0"}, timeout=2).json()["value"] == "0"
        assert requests.get(f"{url}/get", params={"key": "key_1000"}, timeout=2).json()["value"] == "1000"
    finally:
        proc2.terminate()
        proc2.wait()