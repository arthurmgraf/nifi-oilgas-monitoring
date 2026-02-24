"""Fix DuplicateFlowFile processors by replacing with direct multi-connections.

NiFi 1.28.1's DuplicateFlowFile only has a 'success' relationship (not original/duplicate).
This script replaces all DuplicateFlowFile processors with direct connections from the
upstream processor to multiple output ports.
"""

import os
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443")
base = f"{NIFI_URL}/nifi-api"
s = requests.Session()
s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
s.verify = False


def delete_processor_and_connections(pg_id, proc_id):
    """Delete a processor and all its connections in a process group."""
    conns = s.get(f"{base}/process-groups/{pg_id}/connections").json().get("connections", [])
    for conn in conns:
        src = conn["component"]["source"]["id"]
        dst = conn["component"]["destination"]["id"]
        if src == proc_id or dst == proc_id:
            conn_id = conn["id"]
            rev = conn["revision"]["version"]
            try:
                s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                time.sleep(0.3)
            except Exception:
                pass
            resp = s.delete(f"{base}/connections/{conn_id}?version={rev}")
            print(f"  Deleted connection {conn_id[:8]}: {resp.status_code}")

    data = s.get(f"{base}/processors/{proc_id}").json()
    rev = data["revision"]["version"]
    resp = s.delete(f"{base}/processors/{proc_id}?version={rev}")
    print(f"  Deleted processor: {resp.status_code}")


def connect(pg_id, src_id, dst_id, rels, src_type="PROCESSOR", dst_type="OUTPUT_PORT"):
    """Create a connection within a process group."""
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": src_type},
            "destination": {"id": dst_id, "groupId": pg_id, "type": dst_type},
            "selectedRelationships": rels,
        },
    }
    resp = s.post(f"{base}/process-groups/{pg_id}/connections", json=payload)
    print(f"  Connected {src_id[:8]} -> {dst_id[:8]}: {resp.status_code}")
    return resp.status_code in (200, 201)


def get_output_ports(pg_id):
    """Get output port name->id mapping."""
    oports = s.get(f"{base}/process-groups/{pg_id}/output-ports").json().get("outputPorts", [])
    return {p["component"]["name"]: p["component"]["id"] for p in oports}


# Stop root PG
root_id = "83e6edba-019c-1000-c32f-bcc77a22e032"
s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "STOPPED"})
time.sleep(3)
print("Stopped all PGs")

# ============ PG-02: Replace FanOutValidated ============
print("\n=== PG-02: Replace FanOutValidated ===")
pg02_id = "83ffc6b6-019c-1000-97ba-9150e7333c91"
dup02_id = "841f86f5-019c-1000-dca5-4b0485be8f39"
mark_id = "841f86ea-019c-1000-29fe-b84ca9fea35d"

# Check if DuplicateFlowFile still exists
try:
    data = s.get(f"{base}/processors/{dup02_id}").json()
    if "id" in data:
        delete_processor_and_connections(pg02_id, dup02_id)
    else:
        print("  Already deleted")
except Exception:
    print("  Already deleted")

ports02 = get_output_ports(pg02_id)
enrichment_port = ports02.get("enrichment-output")
kafka_port = ports02.get("kafka-output")

# Check if MarkValidated -> enrichment already exists
conns02 = s.get(f"{base}/process-groups/{pg02_id}/connections").json().get("connections", [])
mark_connected = set()
for conn in conns02:
    if conn["component"]["source"]["id"] == mark_id:
        dst_id = conn["component"]["destination"]["id"]
        mark_connected.add(dst_id)

if enrichment_port and enrichment_port not in mark_connected:
    connect(pg02_id, mark_id, enrichment_port, ["success"])
else:
    print("  MarkValidated -> enrichment already connected")

if kafka_port and kafka_port not in mark_connected:
    connect(pg02_id, mark_id, kafka_port, ["success"])
else:
    print("  MarkValidated -> kafka already connected")


# ============ PG-03: Replace FanOutEnriched ============
print("\n=== PG-03: Replace FanOutEnriched ===")
pg03_id = "83ffc708-019c-1000-87de-5e7ed17fa0f1"
dup03_id = "841f87a4-019c-1000-e3fc-4c8be8799eed"
enrich_meta_id = "841f8799-019c-1000-a123-b3b1a30505f0"

try:
    data = s.get(f"{base}/processors/{dup03_id}").json()
    if "id" in data:
        delete_processor_and_connections(pg03_id, dup03_id)
except Exception:
    print("  Already deleted")

ports03 = get_output_ports(pg03_id)
anomaly_port = ports03.get("anomaly-output")
storage_port = ports03.get("storage-output")

conns03 = s.get(f"{base}/process-groups/{pg03_id}/connections").json().get("connections", [])
enrich_connected = set()
for conn in conns03:
    if conn["component"]["source"]["id"] == enrich_meta_id:
        enrich_connected.add(conn["component"]["destination"]["id"])

if anomaly_port and anomaly_port not in enrich_connected:
    connect(pg03_id, enrich_meta_id, anomaly_port, ["success"])
else:
    print("  EnrichMeta -> anomaly already connected")

if storage_port and storage_port not in enrich_connected:
    connect(pg03_id, enrich_meta_id, storage_port, ["success"])
else:
    print("  EnrichMeta -> storage already connected")


# ============ PG-06: Replace FanOutAlerts ============
print("\n=== PG-06: Replace FanOutAlerts ===")
pg06_id = "83ffc75c-019c-1000-8429-b85f3e0aa0a6"
dup06_id = "841f8ac5-019c-1000-32db-88e244074822"
alert_json_id = "841f8abb-019c-1000-da4e-7c6587cafccf"

try:
    data = s.get(f"{base}/processors/{dup06_id}").json()
    if "id" in data:
        delete_processor_and_connections(pg06_id, dup06_id)
except Exception:
    print("  Already deleted")

ports06 = get_output_ports(pg06_id)
kafka06_port = ports06.get("kafka-output")
compliance_port = ports06.get("compliance-output")

conns06 = s.get(f"{base}/process-groups/{pg06_id}/connections").json().get("connections", [])
json_connected = set()
for conn in conns06:
    if conn["component"]["source"]["id"] == alert_json_id:
        json_connected.add(conn["component"]["destination"]["id"])

if kafka06_port and kafka06_port not in json_connected:
    connect(pg06_id, alert_json_id, kafka06_port, ["success"])
else:
    print("  AlertToJSON -> kafka already connected")

if compliance_port and compliance_port not in json_connected:
    connect(pg06_id, alert_json_id, compliance_port, ["success"])
else:
    print("  AlertToJSON -> compliance already connected")


# ============ Start all PGs ============
print("\n=== Starting all PGs ===")
time.sleep(1)
resp = s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "RUNNING"})
print(f"Start: {resp.status_code}")

# Wait and check status
time.sleep(10)
status = s.get(f"{base}/flow/process-groups/{root_id}/status").json()
agg = status.get("processGroupStatus", {}).get("aggregateSnapshot", {})
print(f"\nFlowFiles Queued: {agg.get('flowFilesQueued', 0)}")
print(f"Active Threads:   {agg.get('activeThreadCount', 0)}")

pgs = agg.get("processGroupStatusSnapshots", [])
for pg in sorted(pgs, key=lambda x: x.get("processGroupStatusSnapshot", {}).get("name", "")):
    snap = pg["processGroupStatusSnapshot"]
    name = snap["name"]
    ff_in = snap.get("flowFilesIn", 0)
    ff_out = snap.get("flowFilesOut", 0)
    ff_q = snap.get("flowFilesQueued", 0)
    print(f"  {name}: in={ff_in} out={ff_out} q={ff_q}")
