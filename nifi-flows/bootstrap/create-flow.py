"""NiFi Flow Bootstrap: Programmatically create the Oil & Gas Monitoring flow.

Creates the complete NiFi dataflow via the NiFi REST API, including parameter
contexts, controller services, process groups, ports, and connections. Designed
to run as a one-time setup script after the NiFi Docker container is healthy.

Usage:
    export NIFI_USERNAME=admin
    export NIFI_PASSWORD=admin_password
    python create-flow.py [--nifi-url https://nifi:8443] [--context dev-params]

Environment Variables:
    NIFI_USERNAME: NiFi admin username (required)
    NIFI_PASSWORD: NiFi admin password (required)
    NIFI_URL:      NiFi base URL (default: https://localhost:8443)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nifi-bootstrap")

SCRIPT_DIR = Path(__file__).resolve().parent

PROCESS_GROUPS = [
    {
        "id": "PG-01",
        "name": "PG-01: MQTT Ingestion",
        "position": {"x": 0, "y": 0},
        "comments": "Subscribes to MQTT topics and converts sensor readings to FlowFiles",
    },
    {
        "id": "PG-02",
        "name": "PG-02: Schema Validation",
        "position": {"x": 400, "y": 0},
        "comments": "Validates incoming sensor readings against Avro schemas",
    },
    {
        "id": "PG-03",
        "name": "PG-03: Data Enrichment",
        "position": {"x": 800, "y": 0},
        "comments": "Enriches sensor readings with equipment metadata from PostgreSQL",
    },
    {
        "id": "PG-04",
        "name": "PG-04: Kafka Publishing (Validated)",
        "position": {"x": 400, "y": 400},
        "comments": "Publishes validated sensor readings to Kafka for downstream consumers",
    },
    {
        "id": "PG-05",
        "name": "PG-05: Anomaly Detection",
        "position": {"x": 1200, "y": 0},
        "comments": "Runs threshold, moving average, and rate-of-change anomaly detectors",
    },
    {
        "id": "PG-06",
        "name": "PG-06: Alert Routing & Persistence",
        "position": {"x": 1600, "y": 0},
        "comments": "Routes anomaly alerts by severity and persists to PostgreSQL alert_history",
    },
    {
        "id": "PG-07",
        "name": "PG-07: TimescaleDB Storage",
        "position": {"x": 1200, "y": 400},
        "comments": "Writes enriched sensor readings to TimescaleDB hypertables",
    },
    {
        "id": "PG-08",
        "name": "PG-08: Kafka Publishing (Anomalies)",
        "position": {"x": 2000, "y": 0},
        "comments": "Publishes detected anomaly alerts to Kafka anomaly-alerts topic",
    },
    {
        "id": "PG-09",
        "name": "PG-09: Compliance & Reporting",
        "position": {"x": 2000, "y": 400},
        "comments": "Processes compliance emission events and publishes to compliance topic",
    },
    {
        "id": "PG-10",
        "name": "PG-10: Dead Letter Queue",
        "position": {"x": 1200, "y": 800},
        "comments": "Captures failed records for manual review and reprocessing",
    },
]

CONNECTIONS = [
    {"from": "PG-01", "to": "PG-02", "from_port": "validated-output", "to_port": "input"},
    {"from": "PG-02", "to": "PG-03", "from_port": "enrichment-output", "to_port": "input"},
    {"from": "PG-02", "to": "PG-04", "from_port": "kafka-output", "to_port": "input"},
    {"from": "PG-02", "to": "PG-10", "from_port": "failure-output", "to_port": "input"},
    {"from": "PG-03", "to": "PG-05", "from_port": "anomaly-output", "to_port": "input"},
    {"from": "PG-03", "to": "PG-07", "from_port": "storage-output", "to_port": "input"},
    {"from": "PG-03", "to": "PG-10", "from_port": "failure-output", "to_port": "input"},
    {"from": "PG-05", "to": "PG-06", "from_port": "alert-output", "to_port": "input"},
    {"from": "PG-05", "to": "PG-10", "from_port": "failure-output", "to_port": "input"},
    {"from": "PG-06", "to": "PG-08", "from_port": "kafka-output", "to_port": "input"},
    {"from": "PG-06", "to": "PG-09", "from_port": "compliance-output", "to_port": "input"},
    {"from": "PG-06", "to": "PG-10", "from_port": "failure-output", "to_port": "input"},
]


class NiFiBootstrapError(Exception):
    """Raised when a bootstrap operation fails unrecoverably."""


class NiFiClient:
    """HTTP client for the NiFi REST API with token-based authentication.

    Handles login, token refresh, and provides typed methods for creating
    NiFi resources (parameter contexts, controller services, process groups,
    ports, connections).

    Attributes:
        base_url: The NiFi base URL (e.g., https://nifi:8443).
        session: Configured requests.Session with SSL verification disabled.
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: str | None = None
        self._token_expiry: float = 0.0

        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _ensure_authenticated(self) -> None:
        """Obtain or refresh the NiFi access token (skipped in HTTP mode)."""
        if self.base_url.startswith("http://"):
            return  # No auth needed in unsecured HTTP mode
        if self._token and time.time() < self._token_expiry - 60:
            return

        logger.info("Authenticating with NiFi at %s", self.base_url)
        resp = self.session.post(
            f"{self.base_url}/nifi-api/access/token",
            data={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if resp.status_code != 201:
            raise NiFiBootstrapError(
                f"Authentication failed (HTTP {resp.status_code}): {resp.text}"
            )

        self._token = resp.text.strip()
        self._token_expiry = time.time() + 43200
        self.session.headers["Authorization"] = f"Bearer {self._token}"
        logger.info("Authentication successful, token obtained")

    def _api_get(self, path: str) -> dict[str, Any]:
        """Execute a GET request against the NiFi API."""
        self._ensure_authenticated()
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def _api_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute a POST request against the NiFi API."""
        self._ensure_authenticated()
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.post(url, json=payload)

        if resp.status_code not in (200, 201):
            logger.error("POST %s failed (HTTP %d): %s", path, resp.status_code, resp.text)
            resp.raise_for_status()

        return resp.json()

    def _api_put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute a PUT request against the NiFi API."""
        self._ensure_authenticated()
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.put(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    def wait_for_ready(self, timeout_seconds: int = 300, poll_interval: int = 10) -> None:
        """Wait for NiFi to be fully operational.

        Polls the system-diagnostics endpoint until NiFi responds with HTTP 200.

        Args:
            timeout_seconds: Maximum time to wait before giving up.
            poll_interval: Seconds between poll attempts.

        Raises:
            NiFiBootstrapError: If NiFi does not become ready within the timeout.
        """
        logger.info(
            "Waiting for NiFi to be ready at %s (timeout: %ds)",
            self.base_url,
            timeout_seconds,
        )
        deadline = time.time() + timeout_seconds
        attempt = 0

        while time.time() < deadline:
            attempt += 1
            try:
                resp = self.session.get(
                    f"{self.base_url}/nifi-api/system-diagnostics",
                    timeout=10,
                )
                if resp.status_code == 200:
                    logger.info("NiFi is ready (attempt %d)", attempt)
                    return
                if resp.status_code == 401:
                    try:
                        self._ensure_authenticated()
                        resp = self.session.get(
                            f"{self.base_url}/nifi-api/system-diagnostics",
                            timeout=10,
                        )
                        if resp.status_code == 200:
                            logger.info("NiFi is ready (attempt %d, after auth)", attempt)
                            return
                    except Exception:
                        pass

                logger.debug(
                    "NiFi not ready yet (HTTP %d), retrying in %ds (attempt %d)",
                    resp.status_code,
                    poll_interval,
                    attempt,
                )
            except requests.ConnectionError:
                logger.debug(
                    "NiFi not reachable, retrying in %ds (attempt %d)",
                    poll_interval,
                    attempt,
                )
            except requests.Timeout:
                logger.debug(
                    "NiFi connection timed out, retrying in %ds (attempt %d)",
                    poll_interval,
                    attempt,
                )

            time.sleep(poll_interval)

        raise NiFiBootstrapError(
            f"NiFi did not become ready within {timeout_seconds}s "
            f"at {self.base_url} ({attempt} attempts)"
        )

    def get_root_process_group_id(self) -> str:
        """Retrieve the root process group ID."""
        data = self._api_get("/flow/process-groups/root")
        pg_id: str = data["processGroupFlow"]["id"]
        logger.info("Root process group ID: %s", pg_id)
        return pg_id

    def create_parameter_context(
        self, name: str, description: str, parameters: list[dict[str, Any]]
    ) -> str:
        """Create a parameter context with the given parameters.

        Args:
            name: Parameter context name.
            description: Human-readable description.
            parameters: List of parameter dicts with name, value, sensitive, description.

        Returns:
            The ID of the created parameter context.
        """
        nifi_params = []
        for param in parameters:
            nifi_params.append({
                "parameter": {
                    "name": param["name"],
                    "description": param.get("description", ""),
                    "sensitive": param.get("sensitive", False),
                    "value": param["value"],
                }
            })

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "description": description,
                "parameters": nifi_params,
            },
        }

        result = self._api_post("/parameter-contexts", payload)
        ctx_id: str = result["id"]
        logger.info("Created parameter context '%s' (ID: %s)", name, ctx_id)
        return ctx_id

    def create_controller_service(
        self,
        process_group_id: str,
        name: str,
        service_type: str,
        properties: dict[str, str],
        description: str = "",
    ) -> str:
        """Create a controller service in the given process group.

        Args:
            process_group_id: Parent process group ID.
            name: Controller service name.
            service_type: Fully qualified Java class name.
            properties: Configuration properties.
            description: Human-readable description.

        Returns:
            The ID of the created controller service.
        """
        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": service_type,
                "properties": properties,
                "comments": description,
            },
        }

        result = self._api_post(
            f"/process-groups/{process_group_id}/controller-services",
            payload,
        )
        svc_id: str = result["id"]
        logger.info("Created controller service '%s' (ID: %s)", name, svc_id)
        return svc_id

    def enable_controller_service(self, service_id: str) -> None:
        """Enable a controller service by ID.

        Args:
            service_id: The controller service ID.
        """
        svc_data = self._api_get(f"/controller-services/{service_id}")
        revision = svc_data["revision"]

        payload = {
            "revision": revision,
            "state": "ENABLED",
        }

        try:
            self._api_put(f"/controller-services/{service_id}/run-status", payload)
            logger.info("Enabled controller service %s", service_id)
        except requests.HTTPError as exc:
            logger.warning(
                "Failed to enable controller service %s: %s (may need dependencies)",
                service_id,
                exc,
            )

    def create_process_group(
        self,
        parent_id: str,
        name: str,
        position: dict[str, int],
        comments: str = "",
    ) -> str:
        """Create a child process group.

        Args:
            parent_id: Parent process group ID.
            name: Process group name.
            position: Dict with 'x' and 'y' coordinates.
            comments: Description or operational notes.

        Returns:
            The ID of the created process group.
        """
        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": position,
                "comments": comments,
            },
        }

        result = self._api_post(f"/process-groups/{parent_id}/process-groups", payload)
        pg_id: str = result["id"]
        logger.info("Created process group '%s' (ID: %s)", name, pg_id)
        return pg_id

    def create_input_port(
        self, process_group_id: str, name: str, position: dict[str, int] | None = None
    ) -> str:
        """Create an input port on a process group.

        Args:
            process_group_id: The process group to add the port to.
            name: Port name.
            position: Optional position coordinates.

        Returns:
            The ID of the created input port.
        """
        if position is None:
            position = {"x": 0, "y": 0}

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": position,
                "type": "INPUT_PORT",
            },
        }

        result = self._api_post(f"/process-groups/{process_group_id}/input-ports", payload)
        port_id: str = result["id"]
        logger.info("Created input port '%s' in PG %s (ID: %s)", name, process_group_id, port_id)
        return port_id

    def create_output_port(
        self, process_group_id: str, name: str, position: dict[str, int] | None = None
    ) -> str:
        """Create an output port on a process group.

        Args:
            process_group_id: The process group to add the port to.
            name: Port name.
            position: Optional position coordinates.

        Returns:
            The ID of the created output port.
        """
        if position is None:
            position = {"x": 400, "y": 0}

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": position,
                "type": "OUTPUT_PORT",
            },
        }

        result = self._api_post(f"/process-groups/{process_group_id}/output-ports", payload)
        port_id: str = result["id"]
        logger.info("Created output port '%s' in PG %s (ID: %s)", name, process_group_id, port_id)
        return port_id

    def create_connection(
        self,
        process_group_id: str,
        source_id: str,
        source_group_id: str,
        dest_id: str,
        dest_group_id: str,
        source_type: str = "OUTPUT_PORT",
        dest_type: str = "INPUT_PORT",
        name: str = "",
    ) -> str:
        """Create a connection between two components.

        Args:
            process_group_id: The parent process group containing both endpoints.
            source_id: Source component ID (port or processor).
            source_group_id: Source component's parent process group ID.
            dest_id: Destination component ID (port or processor).
            dest_group_id: Destination component's parent process group ID.
            source_type: Source type (OUTPUT_PORT, PROCESSOR, etc.).
            dest_type: Destination type (INPUT_PORT, PROCESSOR, etc.).
            name: Optional connection name.

        Returns:
            The ID of the created connection.
        """
        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "source": {
                    "id": source_id,
                    "groupId": source_group_id,
                    "type": source_type,
                },
                "destination": {
                    "id": dest_id,
                    "groupId": dest_group_id,
                    "type": dest_type,
                },
            },
        }

        result = self._api_post(f"/process-groups/{process_group_id}/connections", payload)
        conn_id: str = result["id"]
        logger.info("Created connection '%s' (ID: %s)", name or "unnamed", conn_id)
        return conn_id

    def set_parameter_context(self, process_group_id: str, param_context_id: str) -> None:
        """Bind a parameter context to a process group.

        Args:
            process_group_id: The process group ID.
            param_context_id: The parameter context ID.
        """
        pg_data = self._api_get(f"/process-groups/{process_group_id}")
        revision = pg_data["revision"]

        payload = {
            "revision": revision,
            "component": {
                "id": process_group_id,
                "parameterContext": {"id": param_context_id},
            },
        }

        self._api_put(f"/process-groups/{process_group_id}", payload)
        logger.info(
            "Bound parameter context %s to process group %s",
            param_context_id,
            process_group_id,
        )

    def start_process_group(self, process_group_id: str) -> None:
        """Start all components in a process group.

        Args:
            process_group_id: The process group ID.
        """
        payload = {
            "id": process_group_id,
            "state": "RUNNING",
        }

        try:
            self._api_put(f"/flow/process-groups/{process_group_id}", payload)
            logger.info("Started process group %s", process_group_id)
        except requests.HTTPError as exc:
            logger.warning(
                "Failed to start process group %s: %s (may need manual configuration)",
                process_group_id,
                exc,
            )


def load_json_config(filename: str) -> dict[str, Any]:
    """Load a JSON configuration file from the bootstrap directory.

    Args:
        filename: Name of the JSON file in the same directory as this script.

    Returns:
        Parsed JSON content as a dictionary.

    Raises:
        NiFiBootstrapError: If the file is not found or contains invalid JSON.
    """
    filepath = SCRIPT_DIR / filename

    if not filepath.exists():
        raise NiFiBootstrapError(f"Configuration file not found: {filepath}")

    try:
        with open(filepath, encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        raise NiFiBootstrapError(f"Invalid JSON in {filepath}: {exc}") from exc


def create_parameter_contexts(
    client: NiFiClient, context_name: str
) -> str | None:
    """Create parameter contexts from the configuration file.

    Creates only the specified context (by name), returning its ID.
    All contexts defined in the JSON are created if context_name is 'all'.

    Args:
        client: Authenticated NiFi API client.
        context_name: Name of the parameter context to create, or 'all'.

    Returns:
        The ID of the target parameter context, or None if not found.
    """
    config = load_json_config("parameter-contexts.json")
    contexts = config.get("parameterContexts", [])
    target_id: str | None = None

    for ctx in contexts:
        name = ctx["name"]

        if context_name != "all" and name != context_name:
            continue

        ctx_id = client.create_parameter_context(
            name=name,
            description=ctx.get("description", ""),
            parameters=ctx.get("parameters", []),
        )

        if name == context_name:
            target_id = ctx_id

    return target_id


def create_controller_services(
    client: NiFiClient, root_pg_id: str
) -> dict[str, str]:
    """Create controller services from the configuration file.

    Services that reference other services (e.g., AvroReader -> ConfluentSchemaRegistry)
    are resolved by name after all services are created.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID where services are created.

    Returns:
        Dict mapping service name to service ID.
    """
    config = load_json_config("controller-services.json")
    services = config.get("controllerServices", [])
    service_ids: dict[str, str] = {}

    for svc in services:
        name = svc["name"]
        svc_type = svc["type"]
        properties = dict(svc.get("properties", {}))
        description = svc.get("description", "")

        for prop_key, prop_val in properties.items():
            if isinstance(prop_val, str) and prop_val in service_ids:
                properties[prop_key] = service_ids[prop_val]

        svc_id = client.create_controller_service(
            process_group_id=root_pg_id,
            name=name,
            service_type=svc_type,
            properties=properties,
            description=description,
        )
        service_ids[name] = svc_id

    for svc in services:
        if svc.get("state") == "ENABLED":
            svc_id = service_ids[svc["name"]]
            client.enable_controller_service(svc_id)

    return service_ids


def create_process_groups_and_ports(
    client: NiFiClient, root_pg_id: str
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Create all process groups with their input and output ports.

    Each process group gets a standard 'input' port and context-specific
    output ports defined by the CONNECTIONS list.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID.

    Returns:
        Tuple of:
        - Dict mapping PG logical ID (e.g., 'PG-01') to NiFi PG ID.
        - Dict mapping PG logical ID to dict of port_name -> port_id.
    """
    pg_ids: dict[str, str] = {}
    port_ids: dict[str, dict[str, str]] = {}

    for pg_def in PROCESS_GROUPS:
        logical_id = pg_def["id"]

        nifi_pg_id = client.create_process_group(
            parent_id=root_pg_id,
            name=pg_def["name"],
            position=pg_def["position"],
            comments=pg_def.get("comments", ""),
        )
        pg_ids[logical_id] = nifi_pg_id
        port_ids[logical_id] = {}

        input_port_id = client.create_input_port(
            process_group_id=nifi_pg_id,
            name="input",
            position={"x": 0, "y": 200},
        )
        port_ids[logical_id]["input"] = input_port_id

    output_ports_needed: dict[str, set[str]] = {}
    for conn in CONNECTIONS:
        from_pg = conn["from"]
        from_port = conn["from_port"]
        output_ports_needed.setdefault(from_pg, set()).add(from_port)

    for logical_id, port_names in output_ports_needed.items():
        nifi_pg_id = pg_ids[logical_id]
        y_offset = 0

        for port_name in sorted(port_names):
            port_id = client.create_output_port(
                process_group_id=nifi_pg_id,
                name=port_name,
                position={"x": 600, "y": y_offset},
            )
            port_ids[logical_id][port_name] = port_id
            y_offset += 150

    return pg_ids, port_ids


def create_connections(
    client: NiFiClient,
    root_pg_id: str,
    pg_ids: dict[str, str],
    port_ids: dict[str, dict[str, str]],
) -> list[str]:
    """Create connections between process groups via their ports.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID (parent of all connections).
        pg_ids: Mapping of logical PG ID to NiFi PG ID.
        port_ids: Mapping of logical PG ID to dict of port_name -> port_id.

    Returns:
        List of created connection IDs.
    """
    connection_ids: list[str] = []

    for conn in CONNECTIONS:
        from_pg = conn["from"]
        to_pg = conn["to"]
        from_port_name = conn["from_port"]
        to_port_name = conn["to_port"]

        from_pg_id = pg_ids.get(from_pg)
        to_pg_id = pg_ids.get(to_pg)

        if not from_pg_id or not to_pg_id:
            logger.error(
                "Cannot create connection %s -> %s: process group not found",
                from_pg,
                to_pg,
            )
            continue

        from_port_id = port_ids.get(from_pg, {}).get(from_port_name)
        to_port_id = port_ids.get(to_pg, {}).get(to_port_name)

        if not from_port_id:
            logger.error(
                "Cannot create connection: output port '%s' not found on %s",
                from_port_name,
                from_pg,
            )
            continue

        if not to_port_id:
            logger.error(
                "Cannot create connection: input port '%s' not found on %s",
                to_port_name,
                to_pg,
            )
            continue

        conn_name = f"{from_pg}:{from_port_name} -> {to_pg}:{to_port_name}"

        conn_id = client.create_connection(
            process_group_id=root_pg_id,
            source_id=from_port_id,
            source_group_id=from_pg_id,
            dest_id=to_port_id,
            dest_group_id=to_pg_id,
            source_type="OUTPUT_PORT",
            dest_type="INPUT_PORT",
            name=conn_name,
        )
        connection_ids.append(conn_id)

    return connection_ids


def start_all_process_groups(
    client: NiFiClient, pg_ids: dict[str, str]
) -> None:
    """Start all created process groups.

    Args:
        client: Authenticated NiFi API client.
        pg_ids: Mapping of logical PG ID to NiFi PG ID.
    """
    for logical_id in sorted(pg_ids.keys()):
        nifi_pg_id = pg_ids[logical_id]
        client.start_process_group(nifi_pg_id)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Bootstrap NiFi Oil & Gas Monitoring flow via REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment Variables:\n"
            "  NIFI_USERNAME  NiFi admin username (required)\n"
            "  NIFI_PASSWORD  NiFi admin password (required)\n"
            "  NIFI_URL       NiFi base URL (optional, overrides --nifi-url)\n"
        ),
    )
    parser.add_argument(
        "--nifi-url",
        default="http://localhost:8080",
        help="NiFi base URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--context",
        default="dev-params",
        help="Parameter context name to activate (default: dev-params)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Seconds to wait for NiFi readiness (default: 300)",
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Create flow but do not start process groups",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    """Orchestrate the full NiFi flow bootstrap process."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    import os

    nifi_url = os.environ.get("NIFI_URL", args.nifi_url)
    username = os.environ.get("NIFI_USERNAME")
    password = os.environ.get("NIFI_PASSWORD")

    if not username or not password:
        logger.error(
            "NIFI_USERNAME and NIFI_PASSWORD environment variables are required"
        )
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("NiFi Flow Bootstrap - Oil & Gas Monitoring Platform")
    logger.info("=" * 70)
    logger.info("NiFi URL:          %s", nifi_url)
    logger.info("Parameter context: %s", args.context)
    logger.info("Auto-start:        %s", not args.no_start)
    logger.info("=" * 70)

    client = NiFiClient(base_url=nifi_url, username=username, password=password)

    logger.info("[Step 1/7] Waiting for NiFi to be ready...")
    client.wait_for_ready(timeout_seconds=args.timeout)

    logger.info("[Step 2/7] Creating parameter contexts...")
    param_ctx_id = create_parameter_contexts(client, context_name=args.context)
    if not param_ctx_id:
        logger.warning(
            "Parameter context '%s' was not found in configuration; "
            "creating all defined contexts",
            args.context,
        )
        create_parameter_contexts(client, context_name="all")

    logger.info("[Step 3/7] Retrieving root process group...")
    root_pg_id = client.get_root_process_group_id()

    if param_ctx_id:
        logger.info("[Step 3.5/7] Binding parameter context to root process group...")
        client.set_parameter_context(root_pg_id, param_ctx_id)

    logger.info("[Step 4/7] Creating controller services...")
    service_ids = create_controller_services(client, root_pg_id)
    logger.info("Created %d controller services", len(service_ids))

    logger.info("[Step 5/7] Creating process groups and ports...")
    pg_ids, port_ids = create_process_groups_and_ports(client, root_pg_id)
    logger.info("Created %d process groups", len(pg_ids))

    logger.info("[Step 6/7] Creating connections...")
    connection_ids = create_connections(client, root_pg_id, pg_ids, port_ids)
    logger.info("Created %d connections", len(connection_ids))

    if not args.no_start:
        logger.info("[Step 7/7] Starting all process groups...")
        start_all_process_groups(client, pg_ids)
    else:
        logger.info("[Step 7/7] Skipping start (--no-start flag set)")

    logger.info("=" * 70)
    logger.info("Bootstrap complete!")
    logger.info("  Process groups: %d", len(pg_ids))
    logger.info("  Connections:    %d", len(connection_ids))
    logger.info("  Services:       %d", len(service_ids))
    logger.info("=" * 70)
    logger.info("Open NiFi UI at %s/nifi/ to inspect the flow", nifi_url)


if __name__ == "__main__":
    main()
