"""Fix PutSQL quoting issue - NiFi EL single quotes need double-quote wrapping."""

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

# Stop all PGs
print("Stopping all PGs...")
s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "STOPPED"})
time.sleep(5)
print("Stopped")

# Find PutSQL processor in PG-07
pg07_id = "83ffc788-019c-1000-e688-33bc08e3068f"
procs = s.get(f"{base}/process-groups/{pg07_id}/processors").json().get("processors", [])
putsql_id = None
for p in procs:
    if p["component"]["name"] == "WriteToTimescaleDB":
        putsql_id = p["id"]
        break

print(f"PutSQL ID: {putsql_id}")

# Build SQL with proper NiFi EL quoting
# In NiFi EL, string args use double quotes: prepend("'") not prepend(')
dq = chr(34)  # "
sq = chr(39)  # '

def wrap_quote(attr_name):
    """Wraps an attribute value in SQL single quotes using NiFi EL."""
    return "${" + attr_name + ":prepend(" + dq + sq + dq + "):append(" + dq + sq + dq + ")}"

sql = (
    "INSERT INTO sensor_readings "
    "(time, reading_id, platform_id, sensor_id, sensor_type, value, unit, quality_flag) "
    "VALUES ("
    "to_timestamp(${epoch_time:toNumber():divide(1000)}), "
    + wrap_quote("reading_id") + ", "
    + wrap_quote("platform_id") + ", "
    + wrap_quote("sensor_id") + ", "
    + wrap_quote("sensor_type") + ", "
    + "${value}, "
    + wrap_quote("unit") + ", "
    + wrap_quote("quality_flag") + ")"
)

print(f"SQL: {sql}")

# Update PutSQL processor
data = s.get(f"{base}/processors/{putsql_id}").json()
rev = data["revision"]
payload = {
    "revision": rev,
    "component": {
        "id": putsql_id,
        "config": {
            "properties": {
                "putsql-sql-statement": sql,
            }
        }
    }
}
resp = s.put(f"{base}/processors/{putsql_id}", json=payload)
print(f"Updated PutSQL: {resp.status_code}")
if resp.status_code != 200:
    print(f"  {resp.text[:400]}")

# Start all PGs
print("\nStarting all PGs...")
time.sleep(2)
resp = s.put(f"{base}/flow/process-groups/{root_id}", json={"id": root_id, "state": "RUNNING"})
print(f"Start: {resp.status_code}")

# Wait for data
print("Waiting 25s for data to flow...")
time.sleep(25)

# Check TimescaleDB
print("\n=== TimescaleDB ===")
try:
    result = subprocess.run(
        ["docker", "exec", "oilgas-timescaledb", "psql", "-U", "nifi", "-d", "sensordb",
         "-t", "-c", "SELECT COUNT(*) FROM sensor_readings;"],
        capture_output=True, text=True, timeout=15
    )
    count = result.stdout.strip()
    print(f"  Rows: {count}")
except Exception as e:
    print(f"  Error: {e}")

# Check for DB errors
print("\n=== DB Errors ===")
try:
    result = subprocess.run(
        ["docker", "logs", "oilgas-timescaledb", "--since", "20s"],
        capture_output=True, text=True, timeout=10
    )
    lines = [l for l in result.stderr.split("\n") if "ERROR" in l or "STATEMENT" in l]
    for l in lines[:5]:
        print(f"  {l[:250]}")
    if not lines:
        print("  No DB errors in last 20s!")
except Exception as e:
    print(f"  Error: {e}")

# Check Kafka
print("\n=== Kafka ===")
for topic in ["sensor.validated", "alerts.critical", "sensor.dlq"]:
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

# Check bulletins
print("\n=== Recent Bulletins ===")
bulletins = s.get(f"{base}/flow/bulletin-board?limit=8").json()
for b in bulletins.get("bulletinBoard", {}).get("bulletins", [])[:8]:
    bul = b.get("bulletin", {})
    name = bul.get("sourceName", "?")
    msg = bul.get("message", "")[:200]
    print(f"  {name}: {msg}")
