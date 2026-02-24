#!/usr/bin/env python3
"""Fix issues in the reorganized NiFi flow hierarchy.

Fixes two classes of issues:
1. Parameter Context not inherited by child Process Groups
2. Unconnected relationships that need auto-termination

Run: python fix-hierarchy-issues.py --nifi-url http://15.235.61.251:8080
"""

import argparse
import json
import logging
import os
import time
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443")


class NiFi:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    def _req(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self.base}/nifi-api{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            log.error(f"{method} {path} -> {e.code}: {error_body[:300]}")
            raise

    def get(self, path: str) -> dict:
        return self._req("GET", path)

    def put(self, path: str, body: dict) -> dict:
        return self._req("PUT", path, body)

    def get_root_id(self) -> str:
        data = self.get("/flow/process-groups/root")
        return data["processGroupFlow"]["id"]

    def get_pg_flow(self, pg_id: str) -> dict:
        return self.get(f"/flow/process-groups/{pg_id}")

    def get_pg(self, pg_id: str) -> dict:
        return self.get(f"/process-groups/{pg_id}")

    def get_processor(self, proc_id: str) -> dict:
        return self.get(f"/processors/{proc_id}")

    def update_processor(self, proc_id: str, revision: dict, config: dict) -> dict:
        body = {
            "revision": revision,
            "component": {"id": proc_id, "config": config},
        }
        return self.put(f"/processors/{proc_id}", body)

    def update_pg(self, pg_id: str, revision: dict, component: dict) -> dict:
        body = {
            "revision": revision,
            "id": pg_id,
            "component": {"id": pg_id, **component},
        }
        return self.put(f"/process-groups/{pg_id}", body)

    def start_processor(self, proc_id: str, revision: dict) -> dict:
        body = {
            "revision": revision,
            "component": {"id": proc_id, "state": "RUNNING"},
        }
        return self.put(f"/processors/{proc_id}/run-status", body)

    def start_port(self, port_type: str, port_id: str, revision: dict) -> dict:
        body = {
            "revision": revision,
            "component": {"id": port_id, "state": "RUNNING"},
        }
        return self.put(f"/{port_type}/{port_id}/run-status", body)

    def start_pg(self, pg_id: str) -> dict:
        body = {"id": pg_id, "state": "RUNNING"}
        return self.put(f"/flow/process-groups/{pg_id}", body)


def find_all_leaf_pgs(nifi: NiFi, root_id: str) -> list[dict]:
    """Find all leaf (innermost) process groups that contain processors."""
    leaf_pgs = []

    def recurse(pg_id: str, depth: int = 0):
        flow = nifi.get_pg_flow(pg_id)
        pgs = flow["processGroupFlow"]["flow"].get("processGroups", [])
        procs = flow["processGroupFlow"]["flow"].get("processors", [])

        if procs:
            # This PG has processors — it's a leaf
            leaf_pgs.append({
                "id": pg_id,
                "name": flow["processGroupFlow"]["breadcrumb"]["breadcrumb"]["name"],
                "processors": procs,
                "flow": flow["processGroupFlow"]["flow"],
            })

        for pg in pgs:
            recurse(pg["component"]["id"], depth + 1)

    recurse(root_id)
    return leaf_pgs


def fix_parameter_context(nifi: NiFi, pg_id: str, param_context_id: str) -> bool:
    """Set the parameter context on a process group."""
    pg = nifi.get_pg(pg_id)
    current_ctx = pg["component"].get("parameterContext")
    if current_ctx and current_ctx.get("id") == param_context_id:
        return False  # Already set

    revision = pg["revision"]
    nifi.update_pg(pg_id, revision, {
        "parameterContext": {"id": param_context_id},
    })
    return True


def fix_auto_terminated_relationships(nifi: NiFi, proc_data: dict) -> bool:
    """Auto-terminate unconnected relationships on a processor."""
    proc_id = proc_data["component"]["id"]
    proc_name = proc_data["component"]["name"]

    # Get fresh processor data with validation errors
    proc = nifi.get_processor(proc_id)
    comp = proc["component"]
    validation_errors = comp.get("validationErrors", [])

    # Find relationships that need auto-termination
    rels_to_terminate = set()
    for error in validation_errors:
        if "is not connected to any component and is not auto-terminated" in error:
            # Extract relationship name from: 'Relationship X' is invalid because...
            rel_name = error.split("'")[1].replace("Relationship ", "")
            rels_to_terminate.add(rel_name)

    if not rels_to_terminate:
        return False

    # Get current auto-terminated relationships
    current_auto = set(comp.get("config", {}).get("autoTerminatedRelationships", []) or [])
    new_auto = current_auto | rels_to_terminate

    log.info(f"  Auto-terminating on {proc_name}: {rels_to_terminate}")
    revision = proc["revision"]
    nifi.update_processor(proc_id, revision, {
        "autoTerminatedRelationships": list(new_auto),
    })
    return True


def start_stopped_components(nifi: NiFi, leaf_pg: dict) -> int:
    """Start all stopped processors and ports in a leaf PG."""
    started = 0
    pg_id = leaf_pg["id"]

    # Refresh flow
    flow = nifi.get_pg_flow(pg_id)["processGroupFlow"]["flow"]

    # Start stopped processors
    for proc in flow.get("processors", []):
        if proc["component"].get("state") == "STOPPED":
            proc_id = proc["component"]["id"]
            proc_name = proc["component"]["name"]
            # Get fresh revision
            fresh = nifi.get_processor(proc_id)
            run_status = fresh.get("status", {}).get("aggregateSnapshot", {}).get("runStatus", "")
            if run_status == "Invalid":
                log.warning(f"  Skipping {proc_name} — still invalid")
                continue
            try:
                nifi.start_processor(proc_id, fresh["revision"])
                log.info(f"  Started processor: {proc_name}")
                started += 1
            except Exception as e:
                log.error(f"  Failed to start {proc_name}: {e}")

    # Start stopped input ports
    for port in flow.get("inputPorts", []):
        if port["component"].get("state") == "STOPPED":
            port_id = port["component"]["id"]
            port_name = port["component"]["name"]
            try:
                fresh = nifi.get(f"/input-ports/{port_id}")
                nifi.start_port("input-ports", port_id, fresh["revision"])
                log.info(f"  Started input port: {port_name}")
                started += 1
            except Exception as e:
                log.error(f"  Failed to start port {port_name}: {e}")

    # Start stopped output ports
    for port in flow.get("outputPorts", []):
        if port["component"].get("state") == "STOPPED":
            port_id = port["component"]["id"]
            port_name = port["component"]["name"]
            try:
                fresh = nifi.get(f"/output-ports/{port_id}")
                nifi.start_port("output-ports", port_id, fresh["revision"])
                log.info(f"  Started output port: {port_name}")
                started += 1
            except Exception as e:
                log.error(f"  Failed to start port {port_name}: {e}")

    return started


def main():
    parser = argparse.ArgumentParser(description="Fix NiFi hierarchy issues")
    parser.add_argument("--nifi-url", default=NIFI_URL)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fixed")
    args = parser.parse_args()

    nifi = NiFi(args.nifi_url)

    # Get root PG and parameter context
    root_flow = nifi.get_pg_flow("root")
    root_id = root_flow["processGroupFlow"]["id"]
    param_ctx = root_flow["processGroupFlow"].get("parameterContext", {})
    param_ctx_id = param_ctx.get("id")
    log.info(f"Root PG: {root_id}")
    log.info(f"Parameter Context: {param_ctx.get('component', {}).get('name', '?')} ({param_ctx_id})")

    # Phase 1: Find all leaf PGs
    log.info("Phase 1: Discovering leaf Process Groups...")
    leaf_pgs = find_all_leaf_pgs(nifi, root_id)
    log.info(f"  Found {len(leaf_pgs)} leaf PGs")
    for pg in leaf_pgs:
        invalid_count = sum(1 for p in pg["processors"]
                          if p.get("status", {}).get("aggregateSnapshot", {}).get("runStatus") == "Invalid")
        log.info(f"  {pg['name']}: {len(pg['processors'])} processors, {invalid_count} invalid")

    if args.dry_run:
        log.info("DRY RUN — no changes will be made")
        return

    # Phase 2: Set parameter context on all leaf PGs
    log.info("\nPhase 2: Setting parameter context on leaf PGs...")
    ctx_fixed = 0
    for pg in leaf_pgs:
        if fix_parameter_context(nifi, pg["id"], param_ctx_id):
            log.info(f"  Set parameter context on {pg['name']}")
            ctx_fixed += 1
    log.info(f"  Fixed parameter context on {ctx_fixed} PGs")

    # Small delay for NiFi to re-validate
    time.sleep(3)

    # Phase 3: Auto-terminate unconnected relationships
    log.info("\nPhase 3: Auto-terminating unconnected relationships...")
    rels_fixed = 0
    for pg in leaf_pgs:
        # Refresh processor data after parameter context fix
        fresh_flow = nifi.get_pg_flow(pg["id"])
        procs = fresh_flow["processGroupFlow"]["flow"].get("processors", [])
        for proc in procs:
            if fix_auto_terminated_relationships(nifi, proc):
                rels_fixed += 1
    log.info(f"  Fixed auto-termination on {rels_fixed} processors")

    # Small delay for NiFi to re-validate
    time.sleep(3)

    # Phase 4: Start stopped components
    log.info("\nPhase 4: Starting stopped components...")
    total_started = 0
    for pg in leaf_pgs:
        started = start_stopped_components(nifi, pg)
        total_started += started
    log.info(f"  Started {total_started} components")

    # Phase 5: Verify
    log.info("\nPhase 5: Verification...")
    time.sleep(5)

    # Check overall status
    status = nifi.get(f"/flow/process-groups/{root_id}/status")
    snap = status["processGroupStatus"]["aggregateSnapshot"]
    log.info(f"  Active Threads: {snap['activeThreadCount']}")
    log.info(f"  FlowFiles Queued: {snap['flowFilesQueued']}")

    # Check each layer
    root_flow = nifi.get_pg_flow(root_id)
    for pg in root_flow["processGroupFlow"]["flow"].get("processGroups", []):
        comp = pg["component"]
        name = comp["name"]
        running = comp.get("runningCount", 0)
        invalid = comp.get("invalidCount", 0)
        stopped = comp.get("stoppedCount", 0)
        log.info(f"  {name}: running={running}, invalid={invalid}, stopped={stopped}")

    # Check for remaining invalid processors
    remaining_invalid = 0
    for pg in leaf_pgs:
        fresh = nifi.get_pg_flow(pg["id"])
        for proc in fresh["processGroupFlow"]["flow"].get("processors", []):
            rs = proc.get("status", {}).get("aggregateSnapshot", {}).get("runStatus", "")
            if rs == "Invalid":
                remaining_invalid += 1
                comp = proc["component"]
                errors = comp.get("validationErrors", [])
                log.warning(f"  Still invalid: {comp['name']} in {pg['name']}")
                for e in errors:
                    log.warning(f"    {e}")

    if remaining_invalid == 0:
        log.info("\nSUCCESS: All processors are valid and running!")
    else:
        log.warning(f"\n{remaining_invalid} processors still invalid — check errors above")


if __name__ == "__main__":
    main()
