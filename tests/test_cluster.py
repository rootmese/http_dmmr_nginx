import os
import subprocess
import time
import sys

sys.path.insert(0, os.path.dirname(__file__))
from suite_tests import CacheClient, OP_SET, OP_GET

CACHE_BIN = "../http_dmmr_cache/dmmr_cache"

nodes = [
    {"port": 9080, "cluster_port": 9091},
    {"port": 9082, "cluster_port": 9093},
    {"port": 9084, "cluster_port": 9095},
]

seeds = ",".join(f"127.0.0.1:{n['cluster_port']}" for n in nodes)
print(f"Seeds: {seeds}")

# Limpeza
os.system("pkill -9 dmmr_cache || true")
time.sleep(1)
os.system("rm -f ../http_dmmr_cache/apikeys.db || true")
os.system("rm -f apikeys.db || true")

procs = []
for n in nodes:
    cmd = [
        CACHE_BIN,
        "--tcp",
        f"--cluster-port={n['cluster_port']}",
        "--cluster-name=testcluster",
        f"--seeds={seeds}",
        "--advertise=127.0.0.1",
        "--node-id=0"
    ]
    env = os.environ.copy()
    env["DMMR_CACHE_PORT"] = str(n["port"])
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"Started node on cache port {n['port']}, cluster {n['cluster_port']}")
    procs.append((p, n))
    time.sleep(2)
    if p.poll() is not None:
        out, err = p.communicate()
        print(f"Node {n['port']} exited early!")
        print(f"stdout: {out.decode()}")
        print(f"stderr: {err.decode()}")
        sys.exit(1)

print("Waiting 15 seconds for cluster formation...")
time.sleep(15)

# Verifica se todos os nós estão respondendo
for n in nodes:
    try:
        c = CacheClient(use_unix=False, port=n["port"])
        c.send_frame(OP_GET, b"dummy")  # apenas teste de conexão
        print(f"Node {n['port']} is reachable")
    except Exception as e:
        print(f"Node {n['port']} is NOT reachable: {e}")
        sys.exit(1)

# Escreve no primeiro nó
print("Writing key 'cluster_test' to node 9080")
c = CacheClient(use_unix=False, port=9080)
status, _ = c.send_frame(OP_SET, b"cluster_test", b"hello_cluster")
if status != 0:
    print(f"Write failed with status {status}")
    sys.exit(1)

time.sleep(3)

# Verifica em todos os nós
for n in nodes:
    port = n["port"]
    print(f"Checking node {port}...")
    found = False
    for attempt in range(5):
        try:
            c = CacheClient(use_unix=False, port=port)
            status, payload = c.send_frame(OP_GET, b"cluster_test")
            if status == 0 and payload == b"hello_cluster":
                found = True
                print(f"Node {port} has the key")
                break
        except Exception as e:
            print(f"Attempt {attempt+1} on port {port} failed: {e}")
        time.sleep(1)
    if not found:
        print(f"Node {port} did not replicate the key")
        sys.exit(1)

print("Cluster test PASSED")

# Limpeza
for p, _ in procs:
    p.terminate()
    p.wait()
