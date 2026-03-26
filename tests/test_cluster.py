import pytest
import subprocess
import time
import requests
import sys
import os

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

    time.sleep(1.5)
    yield ports

    for proc in procs:
        proc.terminate()
        proc.wait()

def test_replication(cluster):
    ports = cluster
    requests.post(f"http://localhost:{ports[0]}/set", json={"key": "foo", "value": "bar"})
    time.sleep(0.2)
    for port in ports:
        assert requests.get(f"http://localhost:{port}/get", params={"key": "foo"}).json()["value"] == "bar"