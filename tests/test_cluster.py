import pytest
import subprocess
import time
import requests
import sys
import os
import json
from utils import wait_for
from src import config as config

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

def _start_node(port, data_dir, leader_url, nodes_str):
    return subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), leader_url, nodes_str],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

def _wait_healthy(port, timeout=15.0):
    return wait_for(lambda: requests.get(f"http://localhost:{port}/get", params={"key": "__health__"}, timeout=2).status_code == 200, timeout=timeout)

def _all_agree(ports):
    leaders = [requests.get(f"http://localhost:{p}/status", timeout=2).json()["leader"] for p in ports]
    return len(set(leaders)) == 1 and all(leaders)

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
            procs.append(_start_node(port, data_dir, leader, nodes))

        for port in ports:
            assert _wait_healthy(port), f"Node on port {port} did not start in time"

        ok = wait_for(lambda: _all_agree(ports), timeout=10.0)
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
    proc = _start_node(port, data_dir, leader, nodes)
    wait_for(lambda: requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}, timeout=2).json()["value"] == "2", timeout=5.0)
    assert requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}, timeout=2).json()["value"] == "2"
    proc.terminate()
    proc.wait()

def test_replication_log_survives_leader_restart(tmp_path_factory):
    port = 8200
    _kill_port(port)
    url = f"http://localhost:{port}"
    data_dir = tmp_path_factory.mktemp("persist_leader")

    proc = _start_node(port, data_dir, url, url)
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)
    requests.post(f"{url}/set", json={"key": "alpha", "value": "1"}, timeout=5)
    requests.post(f"{url}/set", json={"key": "beta", "value": "2"}, timeout=5)
    proc.terminate()
    proc.wait()

    proc2 = _start_node(port, data_dir, url, url)
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}, timeout=2).status_code == 200)

    follower_port = 8201
    follower_url = f"http://localhost:{follower_port}"
    follower_dir = tmp_path_factory.mktemp("persist_follower")
    follower_proc = _start_node(follower_port, follower_dir, url, f"{url},{follower_url}")

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
    _kill_port(port)
    url = f"http://localhost:{port}"
    data_dir = tmp_path_factory.mktemp("compaction")

    proc = _start_node(port, data_dir, url, url)
    assert _wait_healthy(port), f"Node on port {port} did not start in time"

    for i in range(config.LOG_COMPACTION_THRESHOLD + 2):
        requests.post(f"{url}/set", json={"key": f"key_{i}", "value": str(i)}, timeout=5)

    proc.terminate()
    proc.wait()

    snapshot_path = data_dir / "snapshot.json"
    assert snapshot_path.exists()

    snapshot = json.loads(snapshot_path.read_text())
    assert snapshot["index"] == config.LOG_COMPACTION_THRESHOLD + 1
    assert snapshot["data"]["key_0"] == "0"
    assert snapshot["data"][f"key_{config.LOG_COMPACTION_THRESHOLD}"] == str(config.LOG_COMPACTION_THRESHOLD)

    proc2 = _start_node(port, data_dir, url, url)
    assert _wait_healthy(port), f"Node on port {port} did not start in time"

    try:
        assert requests.get(f"{url}/get", params={"key": "key_0"}, timeout=2).json()["value"] == "0"
        assert requests.get(f"{url}/get", params={"key": f"key_{config.LOG_COMPACTION_THRESHOLD}"}, timeout=2).json()["value"] == str(config.LOG_COMPACTION_THRESHOLD)
    finally:
        proc2.terminate()
        proc2.wait()

def _start_three_node_cluster(tmp_path_factory, ports, prefix):
    leader_url = f"http://localhost:{ports[0]}"
    two_nodes = f"http://localhost:{ports[0]},http://localhost:{ports[1]}"
    all_nodes = ",".join(f"http://localhost:{p}" for p in ports)
    procs = {}

    for port in ports[:2]:
        data_dir = tmp_path_factory.mktemp(f"{prefix}_{port}")
        procs[port] = _start_node(port, data_dir, leader_url, two_nodes)

    for port in ports[:2]:
        assert _wait_healthy(port), f"Node on port {port} did not start in time"

    assert wait_for(lambda: _all_agree(ports[:2]), timeout=10.0), "Initial 2-node cluster did not stabilize"

    new_url = f"http://localhost:{ports[2]}"
    new_dir = tmp_path_factory.mktemp(f"{prefix}_{ports[2]}")
    procs[ports[2]] = _start_node(ports[2], new_dir, leader_url, all_nodes)
    assert _wait_healthy(ports[2]), "New node did not start in time"

    requests.post(f"{leader_url}/add_node", json={"node_url": new_url}, timeout=5)
    assert wait_for(lambda: _all_agree(ports), timeout=10.0), "Cluster did not stabilize after adding node"

    return procs, leader_url, new_url

def test_add_node(tmp_path_factory):
    ports = [8500, 8501, 8502]
    for port in ports:
        _kill_port(port)

    procs, leader_url, new_url = _start_three_node_cluster(tmp_path_factory, ports, "add_node")

    try:
        requests.post(f"{leader_url}/set", json={"key": "after_add", "value": "yes"}, timeout=5)
        ok = wait_for(lambda: requests.get(f"{new_url}/get", params={"key": "after_add"}, timeout=2).json().get("value") == "yes", timeout=10.0)
        assert ok, "New node did not receive replicated write"
    finally:
        for proc in procs.values():
            proc.terminate()
            proc.wait()

def test_remove_node(tmp_path_factory):
    ports = [8500, 8501, 8502]
    for port in ports:
        _kill_port(port)

    procs, leader_url, new_url = _start_three_node_cluster(tmp_path_factory, ports, "remove_node")

    try:
        requests.post(f"{leader_url}/set", json={"key": "after_add", "value": "yes"}, timeout=5)
        ok = wait_for(lambda: requests.get(f"{new_url}/get", params={"key": "after_add"}, timeout=2).json().get("value") == "yes", timeout=10.0)
        assert ok, "New node did not receive replicated write"

        requests.post(f"{leader_url}/remove_node", json={"node_url": new_url}, timeout=5)
        procs[ports[2]].terminate()
        procs[ports[2]].wait()

        assert wait_for(lambda: _all_agree(ports[:2]), timeout=10.0), "2-node cluster did not stabilize after removal"

        result = requests.post(f"{leader_url}/set", json={"key": "after_remove", "value": "yes"}, timeout=5)
        assert result.json().get("ok") is True, "Write failed after node removal"
    finally:
        for port in ports[:2]:
            procs[port].terminate()
            procs[port].wait()
