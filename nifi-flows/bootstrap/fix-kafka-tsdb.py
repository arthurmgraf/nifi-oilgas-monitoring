"""Fix Kafka transaction errors and TimescaleDB column mapping.

Issues:
1. Kafka: Transactions timing out - disable use-transactions
2. TimescaleDB: JSON field 'timestamp' (epoch ms) doesn't match DB column 'time' (TIMESTAMPTZ)
   - Add EvaluateJsonPath to extract timestamp epoch ms value
   - Add ReplaceText to rename 'timestamp' -> 'time' and convert epoch ms to ISO 8601
"""

import requests
import json
import time

base = "http://localhost:8080/nifi-api"
s = requests.Session()
s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})


def update_processor(proc_id, properties=None, auto_terminated=None):
    data = s.get(f"{base}/processors/{proc_id}").json()
    rev = data["revision"]
    name = data["component"]["name"]
    config = {}
    if properties is not None:
        config["properties"] = properties
    if auto_terminated is not None:
        config["autoTerminatedRelationships"] = auto_terminated
    payload = {"revision": rev, "component": {"id": proc_id, "config": config}}
    resp = s.put(f"{base}/processors/{proc_id}", json=payload)
    print(f"  {name}: {resp.status_code}")
    if resp.status_code != 200:
        print(f"    {resp.text[:300]}")
    return resp.status_code == 200


def delete_connection(pg_id, src_id, dst_id):
    """Delete a connection between two processors."""
    conns = s.get(f"{base}/process-groups/{pg_id}/connections").json().get("connections", [])
    for conn in conns:
        src = conn["component"]["source"]["id"]
        dst = conn["component"]["destination"]["id"]
        if src == src_id and dst == dst_id:
            conn_id = conn["id"]
            rev = conn["revision"]["version"]
            try:
                s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                time.sleep(0.5)
            except Exception:
                pass
            resp = s.delete(f"{base}/connections/{conn_id}?version={rev}")
            print(f"  Deleted connection: {resp.status_code}")
            return True
    print("  Connection not found")
    return False


def create_connection(pg_id, src_id, dst_id, rels, src_type="PROCESSOR", dst_type="PROCESSOR"):
    """Create a connection in a process group."""
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": src_type},
            "destination": {"id": dst_id, "groupId": pg_id, "type": dst_type},
            "selectedRelationships": rels,
        },
    }
    resp = s.post(f"{base}/process-groups/{pg_id}/connections", json=payload)
    print(f"  Connected {src_id[:8]} -> {dst_id[:8]} [{','.join(rels)}]: {resp.status_code}")
    if resp.status_code not in (200, 201):
        print(f"    {resp.text[:200]}")
    return resp.status_code in (200, 201)


# ============ Stop root PG ============
root_id = "83e6edba-019c-1000-c32f-bcc77a22e032"
print("Stopping all PGs...")
s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "STOPPED"})
time.sleep(5)
print("Stopped all PGs")

# ============ Fix Kafka publishers: disable transactions ============
print("\n=== Fix Kafka Publishers: disable transactions ===")
kafka_procs = [
    "841f88de-019c-1000-9410-41bb6b5127d6",  # PG-04 PublishToKafka-Validated
    "841f8bf2-019c-1000-6bfc-6469349b645f",  # PG-08 PublishToKafka-Anomalies
    "841f8c8b-019c-1000-3b1d-c41a9c00bbbc",  # PG-09 PublishToKafka-Compliance
    "841f8d22-019c-1000-e4af-2df91a9f29c0",  # PG-10 PublishToKafka-DLQ
]
for proc_id in kafka_procs:
    update_processor(proc_id, properties={
        "use-transactions": "false",
    })

# ============ Fix PG-07: TimescaleDB field mapping ============
print("\n=== Fix PG-07: TimescaleDB field mapping ===")
pg07_id = "83ffc788-019c-1000-e688-33bc08e3068f"

# Get existing processors in PG-07
procs = s.get(f"{base}/process-groups/{pg07_id}/processors").json().get("processors", [])
convert_id = None  # PrepareForDB (ConvertRecord)
put_db_id = None   # WriteToTimescaleDB (PutDatabaseRecord)
log_id = None      # LogDBWriteFailure (LogAttribute)
for p in procs:
    name = p["component"]["name"]
    if name == "PrepareForDB":
        convert_id = p["id"]
    elif name == "WriteToTimescaleDB":
        put_db_id = p["id"]
    elif name == "LogDBWriteFailure":
        log_id = p["id"]

print(f"  PrepareForDB:       {convert_id}")
print(f"  WriteToTimescaleDB: {put_db_id}")
print(f"  LogDBWriteFailure:  {log_id}")

if not convert_id or not put_db_id:
    print("ERROR: Could not find required processors in PG-07")
    exit(1)

# Delete connection: PrepareForDB -> WriteToTimescaleDB
print("\n  Removing PrepareForDB -> WriteToTimescaleDB connection...")
delete_connection(pg07_id, convert_id, put_db_id)

# --- Create EvaluateJsonPath processor ---
# Extracts the epoch ms timestamp from JSON into a FlowFile attribute
print("\n  Creating EvaluateJsonPath (ExtractTimestamp)...")
eval_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "ExtractTimestamp",
        "type": "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "position": {"x": 200, "y": 520},
        "config": {
            "properties": {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "epoch_time": "$.timestamp",
            },
            "autoTerminatedRelationships": ["unmatched"],
        },
        "comments": "Extracts 'timestamp' (epoch ms) into FlowFile attribute 'epoch_time'",
    },
}
resp = s.post(f"{base}/process-groups/{pg07_id}/processors", json=eval_payload)
print(f"  Created EvaluateJsonPath: {resp.status_code}")
if resp.status_code not in (200, 201):
    print(f"    {resp.text[:400]}")
    exit(1)
eval_id = resp.json()["id"]

# --- Create ReplaceText processor ---
# Renames 'timestamp' -> 'time' and converts epoch ms to ISO 8601 string
# Uses NiFi EL: ${epoch_time:toNumber():format("...", "UTC")}
# NiFi's format() on a Number treats it as epoch ms and formats as date
sq = chr(39)  # single quote - avoids Python escaping issues
fmt = f"yyyy-MM-dd{sq}T{sq}HH:mm:ss.SSS{sq}Z{sq}"
replacement_value = f'"time":"${{epoch_time:toNumber():format("{fmt}", "UTC")}}"'
print(f"  Replacement Value: {replacement_value}")

print("\n  Creating ReplaceText (ConvertTimestampToISO)...")
replace_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "ConvertTimestampToISO",
        "type": "org.apache.nifi.processors.standard.ReplaceText",
        "position": {"x": 200, "y": 610},
        "config": {
            "properties": {
                "Regular Expression": '"timestamp"\\s*:\\s*\\d+',
                "Replacement Value": replacement_value,
                "Replacement Strategy": "Regex Replace",
                "Evaluation Mode": "Entire text",
            },
        },
        "comments": "Renames 'timestamp' to 'time' and converts epoch ms to ISO 8601 for TimescaleDB TIMESTAMPTZ",
    },
}
resp = s.post(f"{base}/process-groups/{pg07_id}/processors", json=replace_payload)
print(f"  Created ReplaceText: {resp.status_code}")
if resp.status_code not in (200, 201):
    print(f"    {resp.text[:400]}")
    exit(1)
replace_id = resp.json()["id"]

# --- Wire the new processors ---
print("\n  Wiring processors...")

# PrepareForDB -> EvaluateJsonPath (success)
create_connection(pg07_id, convert_id, eval_id, ["success"])

# EvaluateJsonPath -> ReplaceText (matched)
create_connection(pg07_id, eval_id, replace_id, ["matched"])

# ReplaceText -> WriteToTimescaleDB (success)
create_connection(pg07_id, replace_id, put_db_id, ["success"])

# EvaluateJsonPath failure -> LogDBWriteFailure
if log_id:
    create_connection(pg07_id, eval_id, log_id, ["failure"])

# ReplaceText failure -> LogDBWriteFailure
if log_id:
    create_connection(pg07_id, replace_id, log_id, ["failure"])

print("\n  Pipeline: PrepareForDB -> ExtractTimestamp -> ConvertTimestampToISO -> WriteToTimescaleDB")

# ============ Start all PGs ============
print("\n=== Starting all PGs ===")
time.sleep(2)
resp = s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "RUNNING"})
print(f"Start: {resp.status_code}")

# Wait for data to flow
print("\nWaiting 20s for data to flow...")
time.sleep(20)

# ============ Check flow status ============
print("\n=== Flow Status ===")
status = s.get(f"{base}/flow/process-groups/{root_id}/status").json()
agg = status.get("processGroupStatus", {}).get("aggregateSnapshot", {})
print(f"FlowFiles Queued: {agg.get('flowFilesQueued', 0)}")
print(f"Active Threads:   {agg.get('activeThreadCount', 0)}")

# Check per-PG status
pgs = agg.get("processGroupStatusSnapshots", [])
for pg in sorted(pgs, key=lambda x: x.get("processGroupStatusSnapshot", {}).get("name", "")):
    snap = pg["processGroupStatusSnapshot"]
    name = snap["name"]
    ff_in = snap.get("flowFilesIn", 0)
    ff_out = snap.get("flowFilesOut", 0)
    ff_q = snap.get("flowFilesQueued", 0)
    print(f"  {name}: in={ff_in} out={ff_out} queued={ff_q}")

# Check for bulletins (errors)
print("\n=== Recent Bulletins ===")
bulletins = s.get(f"{base}/flow/bulletin-board?limit=10").json()
for b in bulletins.get("bulletinBoard", {}).get("bulletins", [])[:10]:
    bul = b.get("bulletin", {})
    level = bul.get("level", "")
    source = bul.get("sourceId", "")[:8]
    msg = bul.get("message", "")[:150]
    print(f"  [{level}] {source}: {msg}")
