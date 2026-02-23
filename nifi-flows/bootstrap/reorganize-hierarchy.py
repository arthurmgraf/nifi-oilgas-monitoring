"""NiFi Flow Reorganization: Flat Process Groups → Nested Hierarchy.

Reorganizes 10 flat root-level Process Groups into 5 domain "layers":

  Root Canvas
  ├── 1 - Ingestion Layer      [PG-01, PG-02]
  ├── 2 - Processing Layer     [PG-03, PG-04, PG-05]
  ├── 3 - Storage Layer        [PG-07, PG-09]
  ├── 4 - Alerting Layer       [PG-06, PG-08]
  └── 5 - Error Handling       [PG-10]

Usage:
    python reorganize-hierarchy.py --nifi-url http://15.235.61.251:8080
    python reorganize-hierarchy.py --nifi-url http://15.235.61.251:8080 --dry-run

Prerequisites:
    - NiFi must be running with the 10 flat PGs already created
    - Controller services and parameter contexts must exist at root level
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("reorganize")


# ---------------------------------------------------------------------------
# NiFi REST API Client
# ---------------------------------------------------------------------------
class NiFi:
    """Thin NiFi REST API wrapper."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/") + "/nifi-api"
        self.s = requests.Session()
        self.s.verify = False
        self.s.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    def get(self, path: str) -> dict:
        r = self.s.get(f"{self.base}{path}", timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, data: dict) -> dict:
        r = self.s.post(f"{self.base}{path}", json=data, timeout=30)
        if r.status_code not in (200, 201, 202):
            log.error("POST %s -> %d: %s", path, r.status_code, r.text[:300])
            r.raise_for_status()
        return r.json()

    def put(self, path: str, data: dict) -> dict:
        r = self.s.put(f"{self.base}{path}", json=data, timeout=30)
        if r.status_code not in (200, 201):
            log.error("PUT %s -> %d: %s", path, r.status_code, r.text[:300])
            r.raise_for_status()
        return r.json()

    def delete(self, path: str) -> None:
        r = self.s.delete(f"{self.base}{path}", timeout=30)
        if r.status_code not in (200, 204):
            log.error("DELETE %s -> %d: %s", path, r.status_code, r.text[:300])
            r.raise_for_status()

    # -- Convenience methods --------------------------------------------------

    def root_id(self) -> str:
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]

    def child_pgs(self, parent_id: str) -> list[dict]:
        flow = self.get(f"/flow/process-groups/{parent_id}")
        return flow["processGroupFlow"]["flow"].get("processGroups", [])

    def connections(self, parent_id: str) -> list[dict]:
        flow = self.get(f"/flow/process-groups/{parent_id}")
        return flow["processGroupFlow"]["flow"].get("connections", [])

    def processors(self, pg_id: str) -> list[dict]:
        return self.get(f"/process-groups/{pg_id}/processors").get("processors", [])

    def input_ports(self, pg_id: str) -> list[dict]:
        return self.get(f"/process-groups/{pg_id}/input-ports").get("inputPorts", [])

    def output_ports(self, pg_id: str) -> list[dict]:
        return self.get(f"/process-groups/{pg_id}/output-ports").get("outputPorts", [])

    def internal_connections(self, pg_id: str) -> list[dict]:
        return self.get(f"/process-groups/{pg_id}/connections").get("connections", [])

    def create_pg(self, parent_id: str, name: str, x: int, y: int, comments: str = "") -> str:
        r = self.post(f"/process-groups/{parent_id}/process-groups", {
            "revision": {"version": 0},
            "component": {"name": name, "position": {"x": x, "y": y}, "comments": comments},
        })
        pg_id = r["id"]
        log.info("  Created PG '%s' -> %s", name, pg_id)
        return pg_id

    def create_input_port(self, pg_id: str, name: str, x: int = 0, y: int = 0) -> str:
        r = self.post(f"/process-groups/{pg_id}/input-ports", {
            "revision": {"version": 0},
            "component": {"name": name, "position": {"x": x, "y": y}},
        })
        port_id = r["id"]
        log.info("    Input port '%s' -> %s", name, port_id)
        return port_id

    def create_output_port(self, pg_id: str, name: str, x: int = 0, y: int = 0) -> str:
        r = self.post(f"/process-groups/{pg_id}/output-ports", {
            "revision": {"version": 0},
            "component": {"name": name, "position": {"x": x, "y": y}},
        })
        port_id = r["id"]
        log.info("    Output port '%s' -> %s", name, port_id)
        return port_id

    def create_processor(self, pg_id: str, name: str, proc_type: str,
                         x: int, y: int, config: dict, comments: str = "") -> str:
        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": proc_type,
                "position": {"x": x, "y": y},
                "config": config,
                "comments": comments,
            },
        }
        r = self.post(f"/process-groups/{pg_id}/processors", payload)
        proc_id = r["id"]
        short_type = proc_type.rsplit(".", 1)[-1]
        log.info("    Processor '%s' (%s) -> %s", name, short_type, proc_id)
        return proc_id

    def connect(self, parent_pg_id: str,
                src_id: str, src_group: str, src_type: str,
                dst_id: str, dst_group: str, dst_type: str,
                relationships: list[str],
                back_pressure: int = 10000, back_pressure_size: str = "1 GB") -> str:
        r = self.post(f"/process-groups/{parent_pg_id}/connections", {
            "revision": {"version": 0},
            "component": {
                "source": {"id": src_id, "groupId": src_group, "type": src_type},
                "destination": {"id": dst_id, "groupId": dst_group, "type": dst_type},
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": back_pressure,
                "backPressureDataSizeThreshold": back_pressure_size,
                "flowFileExpiration": "0 sec",
            },
        })
        return r["id"]

    def stop_pg(self, pg_id: str) -> None:
        self.put(f"/flow/process-groups/{pg_id}", {"id": pg_id, "state": "STOPPED"})

    def start_pg(self, pg_id: str) -> None:
        self.put(f"/flow/process-groups/{pg_id}", {"id": pg_id, "state": "RUNNING"})

    def drain_connection(self, conn_id: str) -> None:
        try:
            r = self.post(f"/flowfile-queues/{conn_id}/drop-requests", {})
            drop_id = r.get("dropRequest", {}).get("id", "")
            if not drop_id:
                return
            for _ in range(30):
                time.sleep(1)
                dr = self.get(f"/flowfile-queues/{conn_id}/drop-requests/{drop_id}")
                if dr.get("dropRequest", {}).get("finished", False):
                    break
            self.delete(f"/flowfile-queues/{conn_id}/drop-requests/{drop_id}")
        except Exception as e:
            log.warning("  Drain failed for %s: %s", conn_id, e)

    def delete_connection(self, conn_id: str, max_retries: int = 3) -> None:
        for attempt in range(max_retries):
            data = self.get(f"/connections/{conn_id}")
            rev = data["revision"]["version"]
            queued = data.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
            if queued > 0:
                log.info("    Connection %s has %d queued, draining...", conn_id[:12], queued)
                self.drain_connection(conn_id)
                time.sleep(2)
            try:
                self.delete(f"/connections/{conn_id}?version={rev}")
                return
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 409 and attempt < max_retries - 1:
                    log.warning("    Retry %d: queue not empty, re-draining...", attempt + 1)
                    self.drain_connection(conn_id)
                    time.sleep(5)
                else:
                    raise

    def delete_pg(self, pg_id: str) -> None:
        data = self.get(f"/process-groups/{pg_id}")
        rev = data["revision"]["version"]
        self.delete(f"/process-groups/{pg_id}?version={rev}&disconnectedNodeAcknowledged=false")


# ---------------------------------------------------------------------------
# Phase 1: Export current flow
# ---------------------------------------------------------------------------
def export_flow(nifi: NiFi, root_id: str) -> dict:
    """Export all processor configs from the 10 existing PGs."""
    log.info("Phase 1: Exporting current flow configuration...")
    backup = {"timestamp": datetime.now().isoformat(), "process_groups": {}}

    for pg in nifi.child_pgs(root_id):
        pg_name = pg["component"]["name"]
        pg_id = pg["id"]
        log.info("  Exporting %s (%s)", pg_name, pg_id)

        procs = nifi.processors(pg_id)
        in_ports = nifi.input_ports(pg_id)
        out_ports = nifi.output_ports(pg_id)
        conns = nifi.internal_connections(pg_id)

        backup["process_groups"][pg_name] = {
            "id": pg_id,
            "processors": [{
                "name": p["component"]["name"],
                "type": p["component"]["type"],
                "position": p["position"],
                "config": {
                    k: v for k, v in p["component"]["config"].items()
                    if k in ("schedulingStrategy", "schedulingPeriod", "penaltyDuration",
                             "yieldDuration", "runDurationMillis",
                             "concurrentlySchedulableTaskCount", "properties",
                             "autoTerminatedRelationships")
                },
                "comments": p["component"].get("comments", ""),
            } for p in procs],
            "input_ports": [p["component"]["name"] for p in in_ports],
            "output_ports": [p["component"]["name"] for p in out_ports],
            "connections": [{
                "source_name": c["component"]["source"]["name"],
                "source_type": c["component"]["source"]["type"],
                "dest_name": c["component"]["destination"]["name"],
                "dest_type": c["component"]["destination"]["type"],
                "relationships": c["component"].get("selectedRelationships", []),
                "back_pressure": c["component"].get("backPressureObjectThreshold", 10000),
                "back_pressure_size": c["component"].get("backPressureDataSizeThreshold", "1 GB"),
            } for c in conns],
        }

    log.info("  Exported %d PGs with %d total processors",
             len(backup["process_groups"]),
             sum(len(pg["processors"]) for pg in backup["process_groups"].values()))
    return backup


# ---------------------------------------------------------------------------
# Phase 2: Stop & Drain
# ---------------------------------------------------------------------------
def stop_and_drain(nifi: NiFi, root_id: str) -> None:
    """Stop all processing and drain all queues."""
    log.info("Phase 2: Stopping flow and draining queues...")
    nifi.stop_pg(root_id)
    log.info("  Stopped root PG. Waiting for threads to settle...")
    time.sleep(10)

    # Drain root-level connections
    for conn in nifi.connections(root_id):
        queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
        if queued > 0:
            conn_id = conn["id"]
            log.info("  Draining %s (queued=%d)...", conn_id[:12], queued)
            nifi.drain_connection(conn_id)

    # Drain internal PG connections
    for pg in nifi.child_pgs(root_id):
        pg_id = pg["id"]
        for conn in nifi.internal_connections(pg_id):
            queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
            if queued > 0:
                log.info("  Draining internal queue in %s (queued=%d)...",
                         pg["component"]["name"], queued)
                nifi.drain_connection(conn["id"])

    log.info("  All queues drained.")


# ---------------------------------------------------------------------------
# Phase 3: Delete old structure
# ---------------------------------------------------------------------------
def delete_old_structure(nifi: NiFi, root_id: str) -> None:
    """Delete all root-level connections and PGs."""
    log.info("Phase 3: Deleting old flat structure...")

    # Delete root connections first
    conns = nifi.connections(root_id)
    log.info("  Deleting %d root connections...", len(conns))
    for conn in conns:
        nifi.delete_connection(conn["id"])

    # Delete PGs (cascades to their internal content)
    pgs = nifi.child_pgs(root_id)
    log.info("  Deleting %d process groups...", len(pgs))
    for pg in pgs:
        pg_name = pg["component"]["name"]
        log.info("    Deleting '%s'...", pg_name)
        nifi.delete_pg(pg["id"])

    log.info("  Old structure deleted.")


# ---------------------------------------------------------------------------
# Phase 4: Create nested hierarchy
# ---------------------------------------------------------------------------

# Layer definitions: which old PGs belong to which layer
LAYERS = [
    {
        "name": "1 - Ingestion Layer",
        "comment": "MQTT data ingestion and schema validation",
        "x": 0, "y": 0,
        "children": ["PG-01: MQTT Ingestion", "PG-02: Schema Validation"],
        "input_ports": [],
        "output_ports": ["validated-data", "kafka-data", "failures"],
    },
    {
        "name": "2 - Processing Layer",
        "comment": "Data enrichment, Kafka publishing, and anomaly detection",
        "x": 0, "y": 400,
        "children": ["PG-03: Data Enrichment", "PG-04: Kafka Publishing (Validated)",
                      "PG-05: Anomaly Detection"],
        "input_ports": ["validated-input", "kafka-input"],
        "output_ports": ["storage-data", "alert-data", "failures"],
    },
    {
        "name": "3 - Storage Layer",
        "comment": "TimescaleDB time-series storage and compliance reporting",
        "x": 0, "y": 800,
        "children": ["PG-07: TimescaleDB Storage", "PG-09: Compliance & Reporting"],
        "input_ports": ["storage-input", "compliance-input"],
        "output_ports": [],
    },
    {
        "name": "4 - Alerting Layer",
        "comment": "Alert routing, persistence, and Kafka anomaly publishing",
        "x": 800, "y": 400,
        "children": ["PG-06: Alert Routing & Persistence",
                      "PG-08: Kafka Publishing (Anomalies)"],
        "input_ports": ["alert-input"],
        "output_ports": ["compliance-data", "failures"],
    },
    {
        "name": "5 - Error Handling",
        "comment": "Dead letter queue for all pipeline failures",
        "x": 800, "y": 800,
        "children": ["PG-10: Dead Letter Queue"],
        "input_ports": ["failures-input"],
        "output_ports": [],
    },
]

# Internal wiring within each layer (connects child PG ports to parent ports and between children)
# Format: (src_component, src_port_name, src_type, dst_component, dst_port_name, dst_type, relationships)
# "_PARENT_" means the parent PG's own port
INTERNAL_WIRING = {
    "1 - Ingestion Layer": [
        # PG-01 → PG-02
        ("PG-01: MQTT Ingestion", "validated-output", "OUTPUT_PORT",
         "PG-02: Schema Validation", "input", "INPUT_PORT", [""]),
        # PG-02 outputs → Parent outputs
        ("PG-02: Schema Validation", "enrichment-output", "OUTPUT_PORT",
         "_PARENT_", "validated-data", "OUTPUT_PORT", [""]),
        ("PG-02: Schema Validation", "kafka-output", "OUTPUT_PORT",
         "_PARENT_", "kafka-data", "OUTPUT_PORT", [""]),
        ("PG-02: Schema Validation", "failure-output", "OUTPUT_PORT",
         "_PARENT_", "failures", "OUTPUT_PORT", [""]),
    ],
    "2 - Processing Layer": [
        # Parent inputs → child PGs
        ("_PARENT_", "validated-input", "INPUT_PORT",
         "PG-03: Data Enrichment", "input", "INPUT_PORT", [""]),
        ("_PARENT_", "kafka-input", "INPUT_PORT",
         "PG-04: Kafka Publishing (Validated)", "input", "INPUT_PORT", [""]),
        # PG-03 → PG-05
        ("PG-03: Data Enrichment", "anomaly-output", "OUTPUT_PORT",
         "PG-05: Anomaly Detection", "input", "INPUT_PORT", [""]),
        # Child outputs → Parent outputs
        ("PG-03: Data Enrichment", "storage-output", "OUTPUT_PORT",
         "_PARENT_", "storage-data", "OUTPUT_PORT", [""]),
        ("PG-03: Data Enrichment", "failure-output", "OUTPUT_PORT",
         "_PARENT_", "failures", "OUTPUT_PORT", [""]),
        ("PG-05: Anomaly Detection", "alert-output", "OUTPUT_PORT",
         "_PARENT_", "alert-data", "OUTPUT_PORT", [""]),
        ("PG-05: Anomaly Detection", "failure-output", "OUTPUT_PORT",
         "_PARENT_", "failures", "OUTPUT_PORT", [""]),
    ],
    "3 - Storage Layer": [
        # Parent inputs → child PGs
        ("_PARENT_", "storage-input", "INPUT_PORT",
         "PG-07: TimescaleDB Storage", "input", "INPUT_PORT", [""]),
        ("_PARENT_", "compliance-input", "INPUT_PORT",
         "PG-09: Compliance & Reporting", "input", "INPUT_PORT", [""]),
    ],
    "4 - Alerting Layer": [
        # Parent input → PG-06
        ("_PARENT_", "alert-input", "INPUT_PORT",
         "PG-06: Alert Routing & Persistence", "input", "INPUT_PORT", [""]),
        # PG-06 → PG-08
        ("PG-06: Alert Routing & Persistence", "kafka-output", "OUTPUT_PORT",
         "PG-08: Kafka Publishing (Anomalies)", "input", "INPUT_PORT", [""]),
        # PG-06 outputs → Parent outputs
        ("PG-06: Alert Routing & Persistence", "compliance-output", "OUTPUT_PORT",
         "_PARENT_", "compliance-data", "OUTPUT_PORT", [""]),
        ("PG-06: Alert Routing & Persistence", "failure-output", "OUTPUT_PORT",
         "_PARENT_", "failures", "OUTPUT_PORT", [""]),
    ],
    "5 - Error Handling": [
        # Parent input → PG-10
        ("_PARENT_", "failures-input", "INPUT_PORT",
         "PG-10: Dead Letter Queue", "input", "INPUT_PORT", [""]),
    ],
}

# Root-level connections between parent layers
ROOT_CONNECTIONS = [
    # (src_layer, src_port, dst_layer, dst_port)
    ("1 - Ingestion Layer", "validated-data", "2 - Processing Layer", "validated-input"),
    ("1 - Ingestion Layer", "kafka-data", "2 - Processing Layer", "kafka-input"),
    ("1 - Ingestion Layer", "failures", "5 - Error Handling", "failures-input"),
    ("2 - Processing Layer", "storage-data", "3 - Storage Layer", "storage-input"),
    ("2 - Processing Layer", "alert-data", "4 - Alerting Layer", "alert-input"),
    ("2 - Processing Layer", "failures", "5 - Error Handling", "failures-input"),
    ("4 - Alerting Layer", "compliance-data", "3 - Storage Layer", "compliance-input"),
    ("4 - Alerting Layer", "failures", "5 - Error Handling", "failures-input"),
]


def create_hierarchy(nifi: NiFi, root_id: str, backup: dict) -> None:
    """Create the nested PG hierarchy and recreate all processors."""
    log.info("Phase 4: Creating nested hierarchy...")

    # Track IDs for later wiring
    layer_ids: dict[str, str] = {}          # layer_name -> parent PG id
    layer_ports: dict[str, dict] = {}       # layer_name -> {"input": {name: id}, "output": {name: id}}
    child_ids: dict[str, str] = {}          # child PG name -> child PG id
    child_ports: dict[str, dict] = {}       # child PG name -> {"input": {name: id}, "output": {name: id}}

    for layer in LAYERS:
        layer_name = layer["name"]
        log.info("Creating layer: %s", layer_name)

        # Create parent PG
        parent_id = nifi.create_pg(root_id, layer_name, layer["x"], layer["y"], layer["comment"])
        layer_ids[layer_name] = parent_id
        layer_ports[layer_name] = {"input": {}, "output": {}}

        # Create parent Input Ports
        for i, port_name in enumerate(layer["input_ports"]):
            port_id = nifi.create_input_port(parent_id, port_name, x=i * 400, y=-100)
            layer_ports[layer_name]["input"][port_name] = port_id

        # Create parent Output Ports
        for i, port_name in enumerate(layer["output_ports"]):
            port_id = nifi.create_output_port(parent_id, port_name, x=i * 400, y=900)
            layer_ports[layer_name]["output"][port_name] = port_id

        # Create child PGs inside parent
        for j, child_name in enumerate(layer["children"]):
            log.info("  Creating child: %s", child_name)
            child_x = j * 600
            child_y = 200
            child_id = nifi.create_pg(parent_id, child_name, child_x, child_y)
            child_ids[child_name] = child_id
            child_ports[child_name] = {"input": {}, "output": {}}

            # Get processor definitions from backup
            child_backup = backup["process_groups"].get(child_name, {})
            if not child_backup:
                log.warning("    No backup found for '%s', skipping processors", child_name)
                continue

            # Create child Input Ports
            for port_name in child_backup.get("input_ports", []):
                port_id = nifi.create_input_port(child_id, port_name, x=400, y=-100)
                child_ports[child_name]["input"][port_name] = port_id

            # Create child Output Ports
            for k, port_name in enumerate(child_backup.get("output_ports", [])):
                port_id = nifi.create_output_port(child_id, port_name, x=k * 300, y=700)
                child_ports[child_name]["output"][port_name] = port_id

            # Create Processors
            proc_id_map: dict[str, str] = {}  # old proc name -> new proc id
            for proc in child_backup.get("processors", []):
                proc_config = proc["config"].copy()
                new_id = nifi.create_processor(
                    child_id, proc["name"], proc["type"],
                    proc["position"]["x"], proc["position"]["y"],
                    proc_config, proc.get("comments", ""),
                )
                proc_id_map[proc["name"]] = new_id

            # Create internal connections within child PG
            for conn in child_backup.get("connections", []):
                src_name = conn["source_name"]
                dst_name = conn["dest_name"]
                src_type = conn["source_type"]
                dst_type = conn["dest_type"]

                # Resolve source ID
                if src_type == "INPUT_PORT":
                    src_id = child_ports[child_name]["input"].get(src_name)
                elif src_type == "PROCESSOR":
                    src_id = proc_id_map.get(src_name)
                else:
                    src_id = None

                # Resolve destination ID
                if dst_type == "OUTPUT_PORT":
                    dst_id = child_ports[child_name]["output"].get(dst_name)
                elif dst_type == "PROCESSOR":
                    dst_id = proc_id_map.get(dst_name)
                else:
                    dst_id = None

                if src_id and dst_id:
                    nifi.connect(
                        child_id,
                        src_id, child_id, src_type,
                        dst_id, child_id, dst_type,
                        conn["relationships"],
                        conn.get("back_pressure", 10000),
                        conn.get("back_pressure_size", "1 GB"),
                    )
                else:
                    log.warning("    Could not wire: %s (%s) -> %s (%s)",
                                src_name, src_type, dst_name, dst_type)

    # Phase 4b: Wire internal connections within parent PGs
    log.info("Phase 4b: Wiring internal layer connections...")
    for layer_name, wires in INTERNAL_WIRING.items():
        parent_id = layer_ids[layer_name]
        log.info("  Wiring layer: %s", layer_name)

        for src_comp, src_port, src_type, dst_comp, dst_port, dst_type, rels in wires:
            # Resolve source
            if src_comp == "_PARENT_":
                src_id = layer_ports[layer_name]["input"].get(src_port)
                src_group = parent_id
            else:
                src_id = child_ports[src_comp]["output"].get(src_port)
                src_group = child_ids[src_comp]

            # Resolve destination
            if dst_comp == "_PARENT_":
                dst_id = layer_ports[layer_name]["output"].get(dst_port)
                dst_group = parent_id
            else:
                dst_id = child_ports[dst_comp]["input"].get(dst_port)
                dst_group = child_ids[dst_comp]

            if src_id and dst_id:
                nifi.connect(parent_id, src_id, src_group, src_type,
                             dst_id, dst_group, dst_type, rels)
                log.info("    %s:%s -> %s:%s", src_comp[:20], src_port, dst_comp[:20], dst_port)
            else:
                log.error("    FAILED: %s:%s -> %s:%s (src=%s, dst=%s)",
                          src_comp, src_port, dst_comp, dst_port, src_id, dst_id)

    # Phase 5: Root-level connections between layers
    log.info("Phase 5: Connecting layers at root level...")
    for src_layer, src_port, dst_layer, dst_port in ROOT_CONNECTIONS:
        src_id = layer_ports[src_layer]["output"].get(src_port)
        dst_id = layer_ports[dst_layer]["input"].get(dst_port)
        src_group = layer_ids[src_layer]
        dst_group = layer_ids[dst_layer]

        if src_id and dst_id:
            nifi.connect(root_id, src_id, src_group, "OUTPUT_PORT",
                         dst_id, dst_group, "INPUT_PORT", [""])
            log.info("  %s [%s] -> %s [%s]", src_layer, src_port, dst_layer, dst_port)
        else:
            log.error("  FAILED: %s [%s] -> %s [%s]", src_layer, src_port, dst_layer, dst_port)


# ---------------------------------------------------------------------------
# Phase 6: Start & Verify
# ---------------------------------------------------------------------------
def start_and_verify(nifi: NiFi, root_id: str) -> bool:
    """Start the flow and verify health."""
    log.info("Phase 6: Starting flow...")
    nifi.start_pg(root_id)
    time.sleep(10)

    # Verify structure
    pgs = nifi.child_pgs(root_id)
    conns = nifi.connections(root_id)
    log.info("  Root has %d PGs, %d connections", len(pgs), len(conns))

    for pg in sorted(pgs, key=lambda p: p["component"]["name"]):
        name = pg["component"]["name"]
        status = pg.get("status", {}).get("aggregateSnapshot", {})
        threads = status.get("activeThreadCount", 0)
        queued = status.get("flowFilesQueued", 0)
        children = len(nifi.child_pgs(pg["id"]))
        log.info("  %s: %d children, threads=%d, queued=%d", name, children, threads, queued)

    # Check for bulletins (errors)
    bulletins = nifi.get("/flow/bulletin-board?limit=10")
    bulletin_list = bulletins.get("bulletinBoard", {}).get("bulletins", [])
    if bulletin_list:
        log.warning("  %d bulletins found:", len(bulletin_list))
        for b in bulletin_list[:5]:
            msg = b.get("bulletin", {}).get("message", "")
            log.warning("    %s", msg[:100])
    else:
        log.info("  No bulletins (no errors)")

    return len(pgs) == 5 and len(conns) >= 7


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Reorganize NiFi flow into nested hierarchy")
    parser.add_argument("--nifi-url", default="http://localhost:8080", help="NiFi base URL")
    parser.add_argument("--dry-run", action="store_true", help="Export only, no changes")
    args = parser.parse_args()

    nifi = NiFi(args.nifi_url)
    root_id = nifi.root_id()
    log.info("NiFi root PG: %s", root_id)

    # Verify current structure
    current_pgs = nifi.child_pgs(root_id)
    pg_names = [pg["component"]["name"] for pg in current_pgs]
    log.info("Current PGs (%d): %s", len(current_pgs), pg_names)

    if len(current_pgs) != 10:
        log.error("Expected 10 PGs, found %d. Aborting.", len(current_pgs))
        sys.exit(1)

    # Phase 1: Export
    backup = export_flow(nifi, root_id)

    # Save backup to file
    backup_path = Path(__file__).parent / f"flow-backup-{datetime.now():%Y%m%d-%H%M%S}.json"
    backup_path.write_text(json.dumps(backup, indent=2, default=str))
    log.info("Backup saved to %s", backup_path)

    if args.dry_run:
        log.info("DRY RUN: Would reorganize %d PGs into 5 layers. No changes made.", len(current_pgs))
        return

    # Phase 2: Stop & Drain
    stop_and_drain(nifi, root_id)

    # Phase 3: Delete old structure
    delete_old_structure(nifi, root_id)

    # Phase 4 + 5: Create hierarchy and wire everything
    create_hierarchy(nifi, root_id, backup)

    # Phase 6: Start & Verify
    ok = start_and_verify(nifi, root_id)
    if ok:
        log.info("SUCCESS: Flow reorganized into 5 nested layers!")
    else:
        log.error("VERIFICATION FAILED: Check NiFi UI for issues")
        sys.exit(1)


if __name__ == "__main__":
    main()
