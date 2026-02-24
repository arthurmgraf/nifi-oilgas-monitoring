"""Fix Kafka topics + TimescaleDB timestamp conversion (v2).

Fixes:
1. Kafka topic names: match actual Kafka topics (sensor.validated, alerts.critical, etc.)
2. Kafka DLQ: create missing sensor.dlq topic
3. TimescaleDB: Replace PutDatabaseRecord with EvaluateJsonPath + PutSQL
   - PutSQL uses PostgreSQL to_timestamp() for epoch ms conversion
"""

import os
import subprocess
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443")
base = f"{NIFI_URL}/nifi-api"
s = requests.Session()
s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
s.verify = False

root_id = "83e6edba-019c-1000-c32f-bcc77a22e032"


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


def delete_connection_between(pg_id, src_id, dst_id):
    """Delete connection between two components, draining queue first."""
    conns = s.get(f"{base}/process-groups/{pg_id}/connections").json().get("connections", [])
    for conn in conns:
        src = conn["component"]["source"]["id"]
        dst = conn["component"]["destination"]["id"]
        if src == src_id and dst == dst_id:
            conn_id = conn["id"]
            try:
                s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                time.sleep(1)
            except Exception:
                pass
            conn_data = s.get(f"{base}/connections/{conn_id}").json()
            rev = conn_data["revision"]["version"]
            resp = s.delete(f"{base}/connections/{conn_id}?version={rev}")
            print(f"  Deleted connection: {resp.status_code}")
            return resp.status_code == 200
    return False


def delete_processor(proc_id):
    """Delete a processor."""
    data = s.get(f"{base}/processors/{proc_id}").json()
    if "id" not in data:
        return True
    rev = data["revision"]["version"]
    resp = s.delete(f"{base}/processors/{proc_id}?version={rev}")
    print(f"  Deleted processor {data['component']['name']}: {resp.status_code}")
    return resp.status_code == 200


def create_connection(pg_id, src_id, dst_id, rels, src_type="PROCESSOR", dst_type="PROCESSOR"):
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": src_type},
            "destination": {"id": dst_id, "groupId": pg_id, "type": dst_type},
            "selectedRelationships": rels,
        },
    }
    resp = s.post(f"{base}/process-groups/{pg_id}/connections", json=payload)
    status = "OK" if resp.status_code in (200, 201) else f"FAIL ({resp.status_code})"
    print(f"  Connected [{','.join(rels)}]: {status}")
    if resp.status_code not in (200, 201):
        print(f"    {resp.text[:200]}")
    return resp.status_code in (200, 201)


# ============ Stop all PGs ============
print("Stopping all PGs...")
s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "STOPPED"})
time.sleep(5)
print("Stopped")

# ============ 1. Create sensor.dlq Kafka topic ============
print("\n=== Creating sensor.dlq Kafka topic ===")
try:
    result = subprocess.run(
        ["docker", "exec", "oilgas-kafka", "kafka-topics",
         "--bootstrap-server", "localhost:29092",
         "--create", "--if-not-exists",
         "--topic", "sensor.dlq",
         "--partitions", "3",
         "--replication-factor", "1",
         "--config", "retention.ms=604800000"],
        capture_output=True, text=True, timeout=15
    )
    print(f"  {result.stdout.strip()}")
except Exception as e:
    print(f"  Warning: {e}")

# ============ 2. Fix Kafka publisher topic names ============
print("\n=== Fix Kafka Publisher Topics ===")
topic_fixes = {
    "841f88de-019c-1000-9410-41bb6b5127d6": "sensor.validated",    # PG-04
    "841f8bf2-019c-1000-6bfc-6469349b645f": "alerts.critical",     # PG-08
    "841f8c8b-019c-1000-3b1d-c41a9c00bbbc": "compliance.emissions", # PG-09
    "841f8d22-019c-1000-e4af-2df91a9f29c0": "sensor.dlq",          # PG-10
}
for proc_id, topic in topic_fixes.items():
    update_processor(proc_id, properties={"topic": topic})

# ============ 3. Fix PG-07: Replace PutDatabaseRecord with EvaluateJsonPath + PutSQL ============
print("\n=== Fix PG-07: TimescaleDB Storage ===")
pg07_id = "83ffc788-019c-1000-e688-33bc08e3068f"

# Get existing processors
procs = s.get(f"{base}/process-groups/{pg07_id}/processors").json().get("processors", [])
proc_map = {p["component"]["name"]: p["id"] for p in procs}
print("  Current processors:", list(proc_map.keys()))

convert_id = proc_map.get("PrepareForDB")
put_db_id = proc_map.get("WriteToTimescaleDB")
log_id = proc_map.get("LogDBWriteFailure")
eval_id = proc_map.get("ExtractTimestamp")
replace_id = proc_map.get("ConvertTimestampToISO")

# Delete old processors: ExtractTimestamp, ConvertTimestampToISO, WriteToTimescaleDB
# First delete their connections
print("\n  Removing old processors and connections...")

# Delete all connections involving the processors we want to remove
for proc_name in ["ExtractTimestamp", "ConvertTimestampToISO", "WriteToTimescaleDB"]:
    pid = proc_map.get(proc_name)
    if not pid:
        continue
    conns = s.get(f"{base}/process-groups/{pg07_id}/connections").json().get("connections", [])
    for conn in conns:
        src = conn["component"]["source"]["id"]
        dst = conn["component"]["destination"]["id"]
        if src == pid or dst == pid:
            conn_id = conn["id"]
            try:
                s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                time.sleep(0.5)
            except Exception:
                pass
            conn_data = s.get(f"{base}/connections/{conn_id}").json()
            rev = conn_data["revision"]["version"]
            resp = s.delete(f"{base}/connections/{conn_id}?version={rev}")
            print(f"  Del conn {conn_id[:8]}: {resp.status_code}")
            time.sleep(0.2)

# Delete the processors themselves
for proc_name in ["ExtractTimestamp", "ConvertTimestampToISO", "WriteToTimescaleDB"]:
    pid = proc_map.get(proc_name)
    if pid:
        delete_processor(pid)

# Also delete PrepareForDB -> LogDBWriteFailure connection (we'll re-create later)
conns = s.get(f"{base}/process-groups/{pg07_id}/connections").json().get("connections", [])
for conn in conns:
    src = conn["component"]["source"]["id"]
    dst = conn["component"]["destination"]["id"]
    if src == convert_id:
        conn_id = conn["id"]
        try:
            s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
            time.sleep(0.5)
        except Exception:
            pass
        conn_data = s.get(f"{base}/connections/{conn_id}").json()
        rev = conn_data["revision"]["version"]
        resp = s.delete(f"{base}/connections/{conn_id}?version={rev}")
        print(f"  Del PrepareForDB conn: {resp.status_code}")

# Also delete PrepareForDB (ConvertRecord) - we don't need it
if convert_id:
    # First delete remaining connections
    conns = s.get(f"{base}/process-groups/{pg07_id}/connections").json().get("connections", [])
    for conn in conns:
        if conn["component"]["source"]["id"] == convert_id or conn["component"]["destination"]["id"] == convert_id:
            conn_id = conn["id"]
            try:
                s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                time.sleep(0.3)
            except Exception:
                pass
            conn_data = s.get(f"{base}/connections/{conn_id}").json()
            rev = conn_data["revision"]["version"]
            s.delete(f"{base}/connections/{conn_id}?version={rev}")
    delete_processor(convert_id)

# Get input port
ports = s.get(f"{base}/process-groups/{pg07_id}/input-ports").json().get("inputPorts", [])
input_port_id = ports[0]["id"] if ports else None
print(f"  Input port: {input_port_id}")

# Create EvaluateJsonPath to extract ALL fields into attributes
print("\n  Creating EvaluateJsonPath (ExtractAllFields)...")
eval_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "ExtractAllFields",
        "type": "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "position": {"x": 200, "y": 400},
        "config": {
            "properties": {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "reading_id": "$.reading_id",
                "platform_id": "$.platform_id",
                "sensor_id": "$.sensor_id",
                "sensor_type": "$.sensor_type",
                "value": "$.value",
                "unit": "$.unit",
                "epoch_time": "$.timestamp",
                "quality_flag": "$.quality_flag",
            },
            "autoTerminatedRelationships": ["unmatched"],
        },
        "comments": "Extracts all JSON fields into FlowFile attributes for PutSQL",
    },
}
resp = s.post(f"{base}/process-groups/{pg07_id}/processors", json=eval_payload)
print(f"  Created EvaluateJsonPath: {resp.status_code}")
if resp.status_code not in (200, 201):
    print(f"    {resp.text[:400]}")
    exit(1)
new_eval_id = resp.json()["id"]

# Create PutSQL processor
# The SQL uses to_timestamp() to convert epoch ms to TIMESTAMPTZ
# NiFi EL attributes are interpolated as strings
sq = chr(39)  # single quote for SQL
sql_stmt = (
    f"INSERT INTO sensor_readings (time, reading_id, platform_id, sensor_id, "
    f"sensor_type, value, unit, quality_flag) VALUES ("
    f"to_timestamp(${{epoch_time:toNumber():divide(1000)}}), "
    f"${{reading_id:prepend({sq}):append({sq})}}, "
    f"${{platform_id:prepend({sq}):append({sq})}}, "
    f"${{sensor_id:prepend({sq}):append({sq})}}, "
    f"${{sensor_type:prepend({sq}):append({sq})}}, "
    f"${{value}}, "
    f"${{unit:prepend({sq}):append({sq})}}, "
    f"${{quality_flag:prepend({sq}):append({sq})}})"
)
print(f"  SQL: {sql_stmt[:120]}...")

# Get TimescaleDB DBCP connection pool ID
tsdb_pool_id = "83ffc101-019c-1000-34e4-c49af5bfc412"

print("\n  Creating PutSQL (WriteToTimescaleDB)...")
putsql_payload = {
    "revision": {"version": 0},
    "component": {
        "name": "WriteToTimescaleDB",
        "type": "org.apache.nifi.processors.standard.PutSQL",
        "position": {"x": 200, "y": 520},
        "config": {
            "properties": {
                "JDBC Connection Pool": tsdb_pool_id,
                "putsql-sql-statement": sql_stmt,
                "Support Fragmented Transactions": "false",
                "database-session-autocommit": "true",
                "rollback-on-failure": "false",
            },
            "autoTerminatedRelationships": ["success"],
        },
        "comments": "Inserts sensor readings into TimescaleDB with to_timestamp() conversion",
    },
}
resp = s.post(f"{base}/process-groups/{pg07_id}/processors", json=putsql_payload)
print(f"  Created PutSQL: {resp.status_code}")
if resp.status_code not in (200, 201):
    print(f"    {resp.text[:500]}")
    exit(1)
new_putsql_id = resp.json()["id"]

# Wire: input -> ExtractAllFields -> PutSQL
# failures -> LogDBWriteFailure
print("\n  Wiring processors...")

# input -> EvaluateJsonPath
create_connection(pg07_id, input_port_id, new_eval_id, [], src_type="INPUT_PORT")

# EvaluateJsonPath -> PutSQL (matched)
create_connection(pg07_id, new_eval_id, new_putsql_id, ["matched"])

# EvaluateJsonPath failure -> Log
if log_id:
    create_connection(pg07_id, new_eval_id, log_id, ["failure"])

# PutSQL failure/retry -> Log
if log_id:
    create_connection(pg07_id, new_putsql_id, log_id, ["failure", "retry"])

print("\n  Pipeline: input -> ExtractAllFields -> WriteToTimescaleDB (PutSQL)")

# ============ Start all PGs ============
print("\n=== Starting all PGs ===")
time.sleep(2)
resp = s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "RUNNING"})
print(f"Start: {resp.status_code}")

# Wait for data
print("\nWaiting 25s for data to flow...")
time.sleep(25)

# ============ Check results ============
print("\n=== Flow Status ===")
status = s.get(f"{base}/flow/process-groups/{root_id}/status").json()
agg = status.get("processGroupStatus", {}).get("aggregateSnapshot", {})
print(f"FlowFiles Queued: {agg.get('flowFilesQueued', 0)}")
print(f"Active Threads:   {agg.get('activeThreadCount', 0)}")

pgs = agg.get("processGroupStatusSnapshots", [])
for pg in sorted(pgs, key=lambda x: x.get("processGroupStatusSnapshot", {}).get("name", "")):
    snap = pg["processGroupStatusSnapshot"]
    name = snap["name"]
    ff_in = snap.get("flowFilesIn", 0)
    ff_out = snap.get("flowFilesOut", 0)
    ff_q = snap.get("flowFilesQueued", 0)
    print(f"  {name}: in={ff_in} out={ff_out} q={ff_q}")

# Check TimescaleDB
print("\n=== TimescaleDB ===")
try:
    result = subprocess.run(
        ["docker", "exec", "oilgas-timescaledb", "psql", "-U", "nifi", "-d", "sensordb",
         "-c", "SELECT COUNT(*) as total FROM sensor_readings;"],
        capture_output=True, text=True, timeout=10
    )
    print(f"  {result.stdout.strip()}")
except Exception as e:
    print(f"  Error: {e}")

# Check Kafka
print("\n=== Kafka Topics ===")
for topic in ["sensor.validated", "alerts.critical", "compliance.emissions", "sensor.dlq"]:
    try:
        result = subprocess.run(
            ["docker", "exec", "oilgas-kafka", "kafka-run-class",
             "kafka.tools.GetOffsetShell",
             "--broker-list", "localhost:29092",
             "--topic", topic],
            capture_output=True, text=True, timeout=10
        )
        offsets = result.stdout.strip()
        total = sum(int(line.split(":")[-1]) for line in offsets.split("\n") if line.strip())
        print(f"  {topic}: {total} messages")
    except Exception as e:
        print(f"  {topic}: {e}")

# Bulletins
print("\n=== Recent Bulletins ===")
bulletins = s.get(f"{base}/flow/bulletin-board?limit=10").json()
for b in bulletins.get("bulletinBoard", {}).get("bulletins", [])[:10]:
    bul = b.get("bulletin", {})
    level = bul.get("level", "")
    name = bul.get("sourceName", "?")
    msg = bul.get("message", "")[:180]
    print(f"  [{level}] {name}: {msg}")
