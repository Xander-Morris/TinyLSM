import pytest
import subprocess
import time
import requests
import sys
import os

def wait_for(fn, timeout=3.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False

@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    procs = []
    ports = [8100, 8101, 8102]
    leader = f"http://localhost:{ports[0]}"
    nodes = ",".join(f"http://localhost:{p}" for p in ports)

    for port in ports:
        data_dir = tmp_path_factory.mktemp(f"node_{port}")
        proc = subprocess.Popen(
            [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), leader, nodes],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(proc)

    for port in ports:
        wait_for(lambda p=port: requests.get(f"http://localhost:{p}/get", params={"key": "__health__"}).status_code == 200)
    yield ports

    for proc in procs:
        proc.terminate()
        proc.wait()

def test_replication(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "foo", "value": "bar"})
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "foo"}).json()["value"] == "bar"
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "foo"}).json()["value"] == "bar"

def test_follower_write_forwarded_to_leader(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[1]}/set", json={"key": "follower_write", "value": "yes"})
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "follower_write"}).json()["value"] == "yes"
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "follower_write"}).json()["value"] == "yes"

def test_delete_replicates(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "to_delete", "value": "temp"})
    requests.post(f"http://localhost:{ports[0]}/delete", json={"key": "to_delete"})
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "to_delete"}).json()["value"] is None
        for p in ports
    ))
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "to_delete"}).json()["value"] is None

def test_catchup_after_restart(cluster, tmp_path_factory):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "before_down", "value": "1"})
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "before_down"}).json()["value"] == "1"
        for p in ports
    ))
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "missed_write", "value": "2"})
    wait_for(lambda: all(
        requests.get(f"http://localhost:{p}/get", params={"key": "missed_write"}).json()["value"] == "2"
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
    wait_for(lambda: requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}).json()["value"] == "2", timeout=5.0)
    assert requests.get(f"http://localhost:{port}/get", params={"key": "missed_write"}).json()["value"] == "2"
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
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}).status_code == 200)
    requests.post(f"{url}/set", json={"key": "alpha", "value": "1"})
    requests.post(f"{url}/set", json={"key": "beta", "value": "2"})

    proc.terminate()
    proc.wait()

    proc2 = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(port), str(data_dir), url, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(lambda: requests.get(f"{url}/get", params={"key": "__health__"}).status_code == 200)

    follower_port = 8201
    follower_url = f"http://localhost:{follower_port}"
    follower_dir = tmp_path_factory.mktemp("persist_follower")
    follower_nodes = f"{url},{follower_url}"

    follower_proc = subprocess.Popen(
        [sys.executable, "-m", "src.cluster.node", str(follower_port), str(follower_dir), url, follower_nodes],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    wait_for(lambda: requests.get(f"{follower_url}/get", params={"key": "alpha"}).json()["value"] == "1", timeout=5.0)
    assert requests.get(f"{follower_url}/get", params={"key": "alpha"}).json()["value"] == "1"
    assert requests.get(f"{follower_url}/get", params={"key": "beta"}).json()["value"] == "2"

    proc2.terminate()
    proc2.wait()
    follower_proc.terminate()
    follower_proc.wait()