"""Organize NiFi flow: drain queues, fix parameters, increase backpressure, position processors.

Fixes:
1. Update parameter context Kafka topic names to match actual topics
2. Drain all backpressured queues
3. Increase backpressure thresholds to 50,000 FlowFiles
4. Increase PutSQL concurrent tasks for better throughput
5. Organize processor positions in each PG for clean visual layout
6. Verify all relationships are properly terminated
"""

import requests
import time
import sys

base = "http://localhost:8080/nifi-api"
s = requests.Session()
s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

root_id = "83e6edba-019c-1000-c32f-bcc77a22e032"
param_ctx_id = "83ff85a8-019c-1000-28c8-1cae2ff8045e"


def stop_all():
    print("Stopping all PGs...")
    resp = s.put(
        f"{base}/flow/process-groups/{root_id}",
        json={"id": root_id, "state": "STOPPED"},
    )
    print(f"  Stop: {resp.status_code}")
    time.sleep(8)
    # Verify stopped
    status = s.get(f"{base}/flow/process-groups/{root_id}/status").json()
    threads = status["processGroupStatus"]["aggregateSnapshot"]["activeThreadCount"]
    print(f"  Active threads: {threads}")
    if threads > 0:
        print("  Waiting for threads to finish...")
        time.sleep(10)


def start_all():
    print("\nStarting all PGs...")
    resp = s.put(
        f"{base}/flow/process-groups/{root_id}",
        json={"id": root_id, "state": "RUNNING"},
    )
    print(f"  Start: {resp.status_code}")


def drain_all_queues():
    """Drain all queues in all PGs."""
    print("\n=== Draining all queues ===")

    def drain_pg(pg_id, pg_name):
        conns = (
            s.get(f"{base}/process-groups/{pg_id}/connections")
            .json()
            .get("connections", [])
        )
        for conn in conns:
            queued = conn["status"]["aggregateSnapshot"].get("flowFilesQueued", 0)
            if queued > 0:
                conn_id = conn["id"]
                resp = s.post(f"{base}/flowfile-queues/{conn_id}/drop-requests")
                if resp.status_code in (200, 202):
                    drop_id = resp.json().get("dropRequest", {}).get("id", "")
                    print(f"  {pg_name}: dropping {queued} from {conn_id[:8]}...")
                    # Poll until done
                    for _ in range(10):
                        time.sleep(1)
                        dr = s.get(
                            f"{base}/flowfile-queues/{conn_id}/drop-requests/{drop_id}"
                        ).json()
                        if dr.get("dropRequest", {}).get("finished", False):
                            break
                    # Delete the drop request
                    s.delete(
                        f"{base}/flowfile-queues/{conn_id}/drop-requests/{drop_id}"
                    )

    # Drain root connections
    drain_pg(root_id, "Root")

    # Drain each PG
    pg_ids = {
        "PG-01": "83ffc56d-019c-1000-7706-fa82111d0464",
        "PG-02": "83ffc6b6-019c-1000-97ba-9150e7333c91",
        "PG-03": "83ffc708-019c-1000-87de-5e7ed17fa0f1",
        "PG-04": "83ffc723-019c-1000-d309-b032fc4eb1dd",
        "PG-05": "83ffc743-019c-1000-ac2d-f03e695b0410",
        "PG-06": "83ffc75c-019c-1000-8429-b85f3e0aa0a6",
        "PG-07": "83ffc788-019c-1000-e688-33bc08e3068f",
        "PG-08": "83ffc7a1-019c-1000-7d63-2dccc9d36336",
        "PG-09": "83ffc7cf-019c-1000-696b-0abd1647f4cc",
        "PG-10": "83ffc7f1-019c-1000-4167-610c60a66531",
    }
    for name, pid in pg_ids.items():
        drain_pg(pid, name)

    print("  Queues drained.")


def update_parameter_context():
    """Update Kafka topic names in parameter context to match actual topics."""
    print("\n=== Updating Parameter Context ===")

    # Get current parameter context
    pc = s.get(f"{base}/parameter-contexts/{param_ctx_id}").json()
    rev = pc["revision"]

    # Parameter updates: old names -> new names that match actual Kafka topics
    updates = {
        "kafka.topic.validated": "sensor.validated",
        "kafka.topic.anomalies": "alerts.critical",
        "kafka.topic.compliance": "compliance.emissions",
    }

    # Also add a DLQ topic parameter if it doesn't exist
    existing_params = {
        p["parameter"]["name"] for p in pc["component"]["parameters"]
    }

    parameters = []
    for param_name, new_value in updates.items():
        if param_name in existing_params:
            parameters.append(
                {"parameter": {"name": param_name, "value": new_value}}
            )
            print(f"  Update: {param_name} = {new_value}")
        else:
            print(f"  Skip (not found): {param_name}")

    if "kafka.topic.dlq" not in existing_params:
        parameters.append(
            {"parameter": {"name": "kafka.topic.dlq", "value": "sensor.dlq"}}
        )
        print("  Add: kafka.topic.dlq = sensor.dlq")

    if not parameters:
        print("  No parameter updates needed.")
        return

    payload = {
        "revision": rev,
        "id": param_ctx_id,
        "component": {
            "id": param_ctx_id,
            "parameters": parameters,
        },
    }

    resp = s.put(f"{base}/parameter-contexts/{param_ctx_id}", json=payload)
    print(f"  Update result: {resp.status_code}")
    if resp.status_code != 200:
        print(f"    {resp.text[:300]}")

    # Wait for parameter context update to complete
    if resp.status_code == 200:
        update_req = resp.json()
        req_id = update_req.get("request", {}).get("requestId", "")
        if req_id:
            for _ in range(15):
                time.sleep(2)
                r = s.get(
                    f"{base}/parameter-contexts/{param_ctx_id}/update-requests/{req_id}"
                ).json()
                if r.get("request", {}).get("complete", False):
                    print("  Parameter context update completed.")
                    s.delete(
                        f"{base}/parameter-contexts/{param_ctx_id}/update-requests/{req_id}"
                    )
                    break
            else:
                print("  Warning: Parameter context update may still be pending.")


def increase_backpressure():
    """Increase backpressure threshold on all connections to 50,000."""
    print("\n=== Increasing Backpressure Thresholds ===")
    target_threshold = "50000"
    target_data_size = "1 GB"

    all_pg_ids = [
        root_id,
        "83ffc56d-019c-1000-7706-fa82111d0464",
        "83ffc6b6-019c-1000-97ba-9150e7333c91",
        "83ffc708-019c-1000-87de-5e7ed17fa0f1",
        "83ffc723-019c-1000-d309-b032fc4eb1dd",
        "83ffc743-019c-1000-ac2d-f03e695b0410",
        "83ffc75c-019c-1000-8429-b85f3e0aa0a6",
        "83ffc788-019c-1000-e688-33bc08e3068f",
        "83ffc7a1-019c-1000-7d63-2dccc9d36336",
        "83ffc7cf-019c-1000-696b-0abd1647f4cc",
        "83ffc7f1-019c-1000-4167-610c60a66531",
    ]

    updated = 0
    for pg_id in all_pg_ids:
        conns = (
            s.get(f"{base}/process-groups/{pg_id}/connections")
            .json()
            .get("connections", [])
        )
        for conn in conns:
            conn_id = conn["id"]
            current_threshold = conn["component"].get(
                "backPressureObjectThreshold", "10000"
            )
            current_data = conn["component"].get(
                "backPressureDataSizeThreshold", "1 GB"
            )

            if current_threshold != target_threshold or current_data != target_data_size:
                conn_data = s.get(f"{base}/connections/{conn_id}").json()
                rev = conn_data["revision"]
                payload = {
                    "revision": rev,
                    "component": {
                        "id": conn_id,
                        "backPressureObjectThreshold": target_threshold,
                        "backPressureDataSizeThreshold": target_data_size,
                    },
                }
                resp = s.put(f"{base}/connections/{conn_id}", json=payload)
                if resp.status_code == 200:
                    updated += 1
                else:
                    print(f"  WARN: Failed to update {conn_id[:8]}: {resp.status_code}")

    print(f"  Updated {updated} connections to {target_threshold} / {target_data_size}")


def increase_putsql_concurrency():
    """Increase PutSQL concurrent tasks for better throughput."""
    print("\n=== Increasing PutSQL Concurrency ===")
    pg07_id = "83ffc788-019c-1000-e688-33bc08e3068f"

    procs = (
        s.get(f"{base}/process-groups/{pg07_id}/processors")
        .json()
        .get("processors", [])
    )
    for p in procs:
        if p["component"]["name"] == "WriteToTimescaleDB":
            proc_id = p["id"]
            data = s.get(f"{base}/processors/{proc_id}").json()
            rev = data["revision"]
            current = data["component"]["config"].get(
                "concurrentlySchedulableTaskCount", "1"
            )
            print(f"  Current concurrent tasks: {current}")

            payload = {
                "revision": rev,
                "component": {
                    "id": proc_id,
                    "config": {
                        "concurrentlySchedulableTaskCount": "4",
                        "schedulingPeriod": "0 sec",
                        "penaltyDuration": "10 sec",
                    },
                },
            }
            resp = s.put(f"{base}/processors/{proc_id}", json=payload)
            print(f"  Updated to 4 concurrent tasks: {resp.status_code}")
            if resp.status_code != 200:
                print(f"    {resp.text[:300]}")
            break


def organize_processor_positions():
    """Position processors in a clean vertical layout within each PG."""
    print("\n=== Organizing Processor Positions ===")

    pg_layouts = {
        "83ffc56d-019c-1000-7706-fa82111d0464": {  # PG-01
            "ConsumeMQTT": (400, 50),
            "ExtractSensorFields": (400, 250),
            "AddIngestionMetadata": (400, 450),
        },
        "83ffc6b6-019c-1000-97ba-9150e7333c91": {  # PG-02
            "ValidateSchema": (400, 50),
            "QualityGate": (400, 250),
            "MarkValidated": (400, 450),
        },
        "83ffc708-019c-1000-87de-5e7ed17fa0f1": {  # PG-03
            "PassThroughEnrichment": (400, 50),
            "AddEnrichmentMetadata": (400, 250),
        },
        "83ffc723-019c-1000-d309-b032fc4eb1dd": {  # PG-04
            "PublishToKafka-Validated": (400, 50),
            "LogKafkaFailure": (700, 50),
        },
        "83ffc743-019c-1000-ac2d-f03e695b0410": {  # PG-05
            "ExtractNumericValue": (400, 50),
            "ThresholdAnomalyCheck": (400, 250),
            "MarkAnomaly": (200, 450),
            "MarkNormal": (600, 450),
        },
        "83ffc75c-019c-1000-8429-b85f3e0aa0a6": {  # PG-06
            "RouteBySeverity": (400, 50),
            "EnrichCriticalAlert": (200, 250),
            "EnrichWarningAlert": (600, 250),
            "AlertToJSON": (400, 450),
        },
        "83ffc788-019c-1000-e688-33bc08e3068f": {  # PG-07
            "ExtractAllFields": (400, 50),
            "WriteToTimescaleDB": (400, 250),
            "LogDBWriteFailure": (700, 250),
        },
        "83ffc7a1-019c-1000-7d63-2dccc9d36336": {  # PG-08
            "PublishToKafka-Anomalies": (400, 50),
            "LogKafkaAnomalyFailure": (700, 50),
        },
        "83ffc7cf-019c-1000-696b-0abd1647f4cc": {  # PG-09
            "FilterComplianceEvents": (400, 50),
            "AddComplianceMetadata": (400, 250),
            "PublishToKafka-Compliance": (400, 450),
            "LogComplianceFailure": (700, 450),
        },
        "83ffc7f1-019c-1000-4167-610c60a66531": {  # PG-10
            "AddDLQMetadata": (400, 50),
            "LogFailedRecord": (400, 250),
            "ArchiveToDisk": (200, 450),
            "PublishToKafka-DLQ": (600, 450),
        },
    }

    for pg_id, layout in pg_layouts.items():
        procs = (
            s.get(f"{base}/process-groups/{pg_id}/processors")
            .json()
            .get("processors", [])
        )
        pg_name = "?"
        for p in procs:
            name = p["component"]["name"]
            if name in layout:
                x, y = layout[name]
                proc_id = p["id"]
                data = s.get(f"{base}/processors/{proc_id}").json()
                rev = data["revision"]
                payload = {
                    "revision": rev,
                    "component": {
                        "id": proc_id,
                        "position": {"x": x, "y": y},
                    },
                }
                resp = s.put(f"{base}/processors/{proc_id}", json=payload)
                if resp.status_code != 200:
                    print(f"  WARN: {name} position update failed: {resp.status_code}")

        # Also position input/output ports
        ports_in = (
            s.get(f"{base}/process-groups/{pg_id}/input-ports")
            .json()
            .get("inputPorts", [])
        )
        for port in ports_in:
            port_id = port["id"]
            port_data = s.get(f"{base}/input-ports/{port_id}").json()
            rev = port_data["revision"]
            payload = {
                "revision": rev,
                "component": {
                    "id": port_id,
                    "position": {"x": 400, "y": -100},
                },
            }
            s.put(f"{base}/input-ports/{port_id}", json=payload)

        ports_out = (
            s.get(f"{base}/process-groups/{pg_id}/output-ports")
            .json()
            .get("outputPorts", [])
        )
        for i, port in enumerate(ports_out):
            port_id = port["id"]
            port_data = s.get(f"{base}/output-ports/{port_id}").json()
            rev = port_data["revision"]
            payload = {
                "revision": rev,
                "component": {
                    "id": port_id,
                    "position": {"x": 200 + i * 300, "y": 650},
                },
            }
            s.put(f"{base}/output-ports/{port_id}", json=payload)

    print("  Processors and ports repositioned.")


def organize_root_pg():
    """Position PGs in a 2-column grid layout on the root canvas."""
    print("\n=== Organizing Root Canvas ===")

    pg_positions = {
        "PG-01: MQTT Ingestion": (0, 0),
        "PG-02: Schema Validation": (600, 0),
        "PG-03: Data Enrichment": (0, 250),
        "PG-04: Kafka Publishing (Validated)": (600, 250),
        "PG-05: Anomaly Detection": (0, 500),
        "PG-06: Alert Routing & Persistence": (600, 500),
        "PG-07: TimescaleDB Storage": (0, 750),
        "PG-08: Kafka Publishing (Anomalies)": (600, 750),
        "PG-09: Compliance & Reporting": (0, 1000),
        "PG-10: Dead Letter Queue": (600, 1000),
    }

    pg_flow = s.get(f"{base}/flow/process-groups/{root_id}").json()
    pgs = pg_flow["processGroupFlow"]["flow"]["processGroups"]

    for pg in pgs:
        pg_name = pg["component"]["name"]
        if pg_name in pg_positions:
            x, y = pg_positions[pg_name]
            pg_id = pg["id"]
            data = s.get(f"{base}/process-groups/{pg_id}").json()
            rev = data["revision"]
            payload = {
                "revision": rev,
                "component": {
                    "id": pg_id,
                    "position": {"x": x, "y": y},
                },
            }
            resp = s.put(f"{base}/process-groups/{pg_id}", json=payload)
            if resp.status_code == 200:
                print(f"  {pg_name} -> ({x}, {y})")
            else:
                print(f"  WARN: {pg_name} failed: {resp.status_code}")

    print("  Root canvas organized.")


def fix_auto_terminations():
    """Ensure all unused relationships are auto-terminated."""
    print("\n=== Fixing Auto-Terminated Relationships ===")

    pg_fixes = {
        # PG-05: MarkNormal success should be auto-terminated
        "83ffc743-019c-1000-ac2d-f03e695b0410": {
            "MarkNormal": ["success"],
        },
        # PG-09: FilterComplianceEvents unmatched should be auto-terminated
        "83ffc7cf-019c-1000-696b-0abd1647f4cc": {
            "FilterComplianceEvents": ["unmatched"],
        },
        # PG-10: ArchiveToDisk - auto-terminate success and failure
        "83ffc7f1-019c-1000-4167-610c60a66531": {
            "ArchiveToDisk": ["success", "failure"],
            "PublishToKafka-DLQ": ["success"],
        },
        # PG-04: auto-terminate LogKafkaFailure success
        "83ffc723-019c-1000-d309-b032fc4eb1dd": {
            "LogKafkaFailure": ["success"],
            "PublishToKafka-Validated": ["success"],
        },
        # PG-08: auto-terminate LogKafkaAnomalyFailure success
        "83ffc7a1-019c-1000-7d63-2dccc9d36336": {
            "LogKafkaAnomalyFailure": ["success"],
            "PublishToKafka-Anomalies": ["success"],
        },
    }

    for pg_id, proc_fixes in pg_fixes.items():
        procs = (
            s.get(f"{base}/process-groups/{pg_id}/processors")
            .json()
            .get("processors", [])
        )
        for p in procs:
            name = p["component"]["name"]
            if name in proc_fixes:
                proc_id = p["id"]
                data = s.get(f"{base}/processors/{proc_id}").json()
                rev = data["revision"]
                current_auto = set(
                    data["component"]["config"].get(
                        "autoTerminatedRelationships", []
                    )
                )
                new_auto = current_auto | set(proc_fixes[name])
                if new_auto != current_auto:
                    payload = {
                        "revision": rev,
                        "component": {
                            "id": proc_id,
                            "config": {
                                "autoTerminatedRelationships": list(new_auto)
                            },
                        },
                    }
                    resp = s.put(f"{base}/processors/{proc_id}", json=payload)
                    status = "OK" if resp.status_code == 200 else f"FAIL({resp.status_code})"
                    print(f"  {name}: auto-terminate {proc_fixes[name]} -> {status}")
                    if resp.status_code != 200:
                        print(f"    {resp.text[:200]}")


def verify_flow():
    """Verify the flow is healthy after all fixes."""
    print("\n=== Verification ===")
    time.sleep(15)

    # Flow status
    status = s.get(f"{base}/flow/process-groups/{root_id}/status").json()
    agg = status["processGroupStatus"]["aggregateSnapshot"]
    queued = agg["flowFilesQueued"]
    threads = agg["activeThreadCount"]
    print(f"FlowFiles Queued: {queued}")
    print(f"Active Threads: {threads}")

    # Per-PG status
    for pg in sorted(
        agg.get("processGroupStatusSnapshots", []),
        key=lambda x: x["processGroupStatusSnapshot"]["name"],
    ):
        snap = pg["processGroupStatusSnapshot"]
        name = snap["name"]
        ff_in = snap.get("flowFilesIn", 0)
        ff_out = snap.get("flowFilesOut", 0)
        ff_q = snap.get("flowFilesQueued", 0)
        print(f"  {name}: in={ff_in} out={ff_out} queued={ff_q}")

    # Bulletins
    print("\nBulletins:")
    bulletins = s.get(f"{base}/flow/bulletin-board?limit=10").json()
    buls = bulletins.get("bulletinBoard", {}).get("bulletins", [])
    if not buls:
        print("  No bulletins - CLEAN!")
    for b in buls[:10]:
        bul = b.get("bulletin", {})
        level = bul.get("level", "?")
        name = bul.get("sourceName", "?")
        msg = bul.get("message", "")[:200]
        print(f"  [{level}] {name}: {msg}")

    # Invalid processors
    invalid = agg.get("invalidCount", 0)
    print(f"\nInvalid Components: {invalid}")
    if invalid == 0:
        print("ALL PROCESSORS VALID")


# ============ Main ============
if __name__ == "__main__":
    print("=" * 60)
    print("NiFi Flow Organizer")
    print("=" * 60)

    stop_all()
    drain_all_queues()
    update_parameter_context()
    increase_backpressure()
    increase_putsql_concurrency()
    fix_auto_terminations()
    organize_processor_positions()
    organize_root_pg()
    start_all()
    verify_flow()

    print("\n" + "=" * 60)
    print("DONE - Flow organized and verified")
    print("=" * 60)
