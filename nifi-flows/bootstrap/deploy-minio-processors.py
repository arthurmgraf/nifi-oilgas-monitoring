"""Deploy MinIO S3 integration processors to a running NiFi instance.

Adds PutS3Object processors for MinIO archival to the Storage Layer (PG-07)
and Error Handling / Dead Letter Queue (PG-10) process groups. Also ensures
the required parameter context parameters and MinIO-S3-Credentials controller
service exist.

Usage:
    python deploy-minio-processors.py --username admin --password admin_password
    python deploy-minio-processors.py --nifi-url https://nifi:8443 --username admin --password secret
    python deploy-minio-processors.py --dry-run

Environment Variables:
    NIFI_URL:      NiFi base URL (default: https://localhost:8443)
    NIFI_USERNAME: NiFi admin username (overrides --username)
    NIFI_PASSWORD: NiFi admin password (overrides --password)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nifi-minio-deploy")

# ---------------------------------------------------------------------------
# Known IDs from the live NiFi server
# ---------------------------------------------------------------------------
ROOT_PG_ID = "83e6edba-019c-1000-c32f-bcc77a22e032"
PARAMETER_CONTEXT_ID = "83ff85a8-019c-1000-28c8-1cae2ff8045e"

# ---------------------------------------------------------------------------
# MinIO parameter definitions to add if missing
# ---------------------------------------------------------------------------
MINIO_PARAMETERS = [
    {
        "name": "minio.bucket.data",
        "description": "MinIO bucket for sensor data archival",
        "value": "sensor-data",
        "sensitive": False,
    },
    {
        "name": "minio.bucket.dlq",
        "description": "MinIO bucket for dead letter queue records",
        "value": "sensor-dlq",
        "sensitive": False,
    },
    {
        "name": "minio.bucket.backups",
        "description": "MinIO bucket for backup snapshots",
        "value": "sensor-backups",
        "sensitive": False,
    },
    {
        "name": "minio.region",
        "description": "S3 region (us-east-1 for MinIO compatibility)",
        "value": "us-east-1",
        "sensitive": False,
    },
]


class NiFiDeployError(Exception):
    """Raised when a deployment operation fails unrecoverably."""


class NiFiClient:
    """HTTP client for the NiFi REST API with token-based authentication.

    Handles login, token refresh, and provides typed methods for interacting
    with NiFi resources (parameter contexts, controller services, processors,
    process groups, connections).

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
            raise NiFiDeployError(
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
            logger.error("POST %s failed (HTTP %d): %s", path, resp.status_code, resp.text[:500])
            resp.raise_for_status()

        return resp.json()

    def _api_put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute a PUT request against the NiFi API."""
        self._ensure_authenticated()
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.put(url, json=payload)

        if resp.status_code not in (200, 201):
            logger.error("PUT %s failed (HTTP %d): %s", path, resp.status_code, resp.text[:500])
            resp.raise_for_status()

        return resp.json()

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------
    def wait_for_ready(self, timeout_seconds: int = 300, poll_interval: int = 10) -> None:
        """Wait for NiFi to be fully operational.

        Polls the system-diagnostics endpoint until NiFi responds with HTTP 200.

        Args:
            timeout_seconds: Maximum time to wait before giving up.
            poll_interval: Seconds between poll attempts.

        Raises:
            NiFiDeployError: If NiFi does not become ready within the timeout.
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

        raise NiFiDeployError(
            f"NiFi did not become ready within {timeout_seconds}s "
            f"at {self.base_url} ({attempt} attempts)"
        )

    # ------------------------------------------------------------------
    # Process Group discovery
    # ------------------------------------------------------------------
    def get_root_process_group_id(self) -> str:
        """Retrieve the root process group ID."""
        data = self._api_get("/flow/process-groups/root")
        pg_id: str = data["processGroupFlow"]["id"]
        logger.info("Root process group ID: %s", pg_id)
        return pg_id

    def get_child_process_groups(self, parent_id: str) -> dict[str, str]:
        """Return dict mapping PG name -> PG ID for all children of a parent.

        Args:
            parent_id: Parent process group ID.

        Returns:
            Dict mapping child process group name to its NiFi ID.
        """
        data = self._api_get(f"/flow/process-groups/{parent_id}")
        pgs = data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
        result: dict[str, str] = {}
        for pg in pgs:
            comp = pg.get("component", {})
            result[comp["name"]] = comp["id"]
        return result

    def find_process_group(
        self, parent_id: str, name_contains: list[str]
    ) -> tuple[str, str] | None:
        """Find a child process group whose name contains any of the given substrings.

        Searches children of `parent_id` for a PG whose name contains at least
        one of the strings in `name_contains` (case-insensitive).

        Args:
            parent_id: Parent process group ID.
            name_contains: List of substrings to match against PG names.

        Returns:
            Tuple of (pg_name, pg_id) if found, None otherwise.
        """
        children = self.get_child_process_groups(parent_id)
        for pg_name, pg_id in children.items():
            for needle in name_contains:
                if needle.lower() in pg_name.lower():
                    logger.info("Found process group '%s' (ID: %s)", pg_name, pg_id)
                    return pg_name, pg_id
        return None

    def find_process_group_recursive(
        self, parent_id: str, name_contains: list[str], max_depth: int = 3
    ) -> tuple[str, str] | None:
        """Recursively search for a process group by name substring.

        Searches the parent and all nested children up to `max_depth` levels.

        Args:
            parent_id: Starting parent process group ID.
            name_contains: List of substrings to match against PG names.
            max_depth: Maximum recursion depth.

        Returns:
            Tuple of (pg_name, pg_id) if found, None otherwise.
        """
        # Check direct children first
        result = self.find_process_group(parent_id, name_contains)
        if result:
            return result

        if max_depth <= 0:
            return None

        # Recurse into children
        children = self.get_child_process_groups(parent_id)
        for child_name, child_id in children.items():
            result = self.find_process_group_recursive(
                child_id, name_contains, max_depth - 1
            )
            if result:
                return result

        return None

    # ------------------------------------------------------------------
    # Parameter Context management
    # ------------------------------------------------------------------
    def get_parameter_context(self, ctx_id: str) -> dict[str, Any]:
        """Retrieve a parameter context by ID.

        Args:
            ctx_id: Parameter context ID.

        Returns:
            Full parameter context response dict.
        """
        return self._api_get(f"/parameter-contexts/{ctx_id}")

    def update_parameter_context(
        self,
        ctx_id: str,
        revision: dict[str, Any],
        parameters_to_add: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Submit a parameter context update request to add new parameters.

        Uses the asynchronous NiFi parameter context update API. Parameters
        that already exist in the context are skipped.

        Args:
            ctx_id: Parameter context ID.
            revision: Current revision dict from GET response.
            parameters_to_add: List of parameter dicts with name, value, description, sensitive.

        Returns:
            The update request response dict.
        """
        nifi_params = []
        for param in parameters_to_add:
            nifi_params.append({
                "parameter": {
                    "name": param["name"],
                    "description": param.get("description", ""),
                    "sensitive": param.get("sensitive", False),
                    "value": param["value"],
                }
            })

        payload = {
            "revision": revision,
            "id": ctx_id,
            "parameterContext": {
                "id": ctx_id,
                "parameters": nifi_params,
            },
        }

        result = self._api_post(f"/parameter-contexts/{ctx_id}/update-requests", payload)
        request_id = result.get("request", {}).get("requestId", "unknown")
        logger.info("Submitted parameter context update request: %s", request_id)

        # Poll for completion
        self._wait_for_param_update(ctx_id, request_id)
        return result

    def _wait_for_param_update(
        self, ctx_id: str, request_id: str, timeout: int = 60
    ) -> None:
        """Poll a parameter context update request until it completes.

        Args:
            ctx_id: Parameter context ID.
            request_id: The update request ID.
            timeout: Maximum seconds to wait.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self._api_get(
                f"/parameter-contexts/{ctx_id}/update-requests/{request_id}"
            )
            req = status.get("request", {})
            if req.get("complete", False):
                if req.get("failureReason"):
                    logger.error(
                        "Parameter context update failed: %s", req["failureReason"]
                    )
                else:
                    logger.info("Parameter context update completed successfully")
                # Delete the completed request
                try:
                    self._ensure_authenticated()
                    url = (
                        f"{self.base_url}/nifi-api/parameter-contexts/{ctx_id}"
                        f"/update-requests/{request_id}"
                    )
                    self.session.delete(url)
                except Exception:
                    pass
                return
            time.sleep(2)

        logger.warning(
            "Parameter context update did not complete within %ds", timeout
        )

    # ------------------------------------------------------------------
    # Controller Service management
    # ------------------------------------------------------------------
    def get_controller_services(self, pg_id: str) -> dict[str, str]:
        """Return dict mapping controller service name -> service ID.

        Args:
            pg_id: Process group ID to query.

        Returns:
            Dict mapping service name to service ID.
        """
        data = self._api_get(f"/flow/process-groups/{pg_id}/controller-services")
        svcs = data.get("controllerServices", [])
        result: dict[str, str] = {}
        for svc in svcs:
            comp = svc.get("component", {})
            result[comp["name"]] = svc["id"]
        return result

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

    # ------------------------------------------------------------------
    # Processor management
    # ------------------------------------------------------------------
    def get_processors(self, pg_id: str) -> list[dict[str, Any]]:
        """Return list of existing processors in a process group.

        Args:
            pg_id: Process group ID.

        Returns:
            List of processor dicts from the NiFi API.
        """
        data = self._api_get(f"/process-groups/{pg_id}/processors")
        return data.get("processors", [])

    def create_processor(
        self,
        pg_id: str,
        name: str,
        processor_type: str,
        position: dict[str, int],
        properties: dict[str, str] | None = None,
        auto_terminated: list[str] | None = None,
        scheduling_strategy: str = "TIMER_DRIVEN",
        scheduling_period: str = "0 sec",
        penalty_duration: str = "30 sec",
        yield_duration: str = "1 sec",
        run_duration_nanos: int = 0,
        concurrency: int = 1,
        comments: str = "",
    ) -> dict[str, Any]:
        """Create a processor in the given process group.

        Args:
            pg_id: Parent process group ID.
            name: Processor display name.
            processor_type: Fully qualified Java class name.
            position: Dict with 'x' and 'y' coordinates.
            properties: Processor configuration properties.
            auto_terminated: List of relationship names to auto-terminate.
            scheduling_strategy: Scheduling strategy (TIMER_DRIVEN, CRON_DRIVEN, etc.).
            scheduling_period: How often the processor runs.
            penalty_duration: Penalty duration for failed FlowFiles.
            yield_duration: Yield duration when processor has no work.
            run_duration_nanos: Run duration in nanoseconds (0 = run once per trigger).
            concurrency: Number of concurrent tasks.
            comments: Processor description.

        Returns:
            Full processor response dict (includes 'id').
        """
        config: dict[str, Any] = {
            "schedulingStrategy": scheduling_strategy,
            "schedulingPeriod": scheduling_period,
            "penaltyDuration": penalty_duration,
            "yieldDuration": yield_duration,
            "runDurationMillis": run_duration_nanos // 1_000_000 if run_duration_nanos else 0,
            "concurrentlySchedulableTaskCount": str(concurrency),
        }

        if properties:
            config["properties"] = properties
        if auto_terminated:
            config["autoTerminatedRelationships"] = auto_terminated

        payload = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": processor_type,
                "position": position,
                "config": config,
                "comments": comments,
            },
        }

        result = self._api_post(f"/process-groups/{pg_id}/processors", payload)
        proc_id = result["id"]
        logger.info(
            "  Created processor '%s' (%s) -> %s",
            name,
            processor_type.split(".")[-1],
            proc_id,
        )
        return result

    def create_connection_within_pg(
        self,
        pg_id: str,
        source_id: str,
        dest_id: str,
        relationships: list[str],
        source_type: str = "PROCESSOR",
        dest_type: str = "PROCESSOR",
        name: str = "",
        back_pressure_count: int = 10000,
        back_pressure_size: str = "1 GB",
        flow_file_expiration: str = "0 sec",
    ) -> str:
        """Create a connection within a process group between processors/ports.

        Args:
            pg_id: Parent process group ID.
            source_id: Source component ID.
            dest_id: Destination component ID.
            relationships: List of relationship names to route through this connection.
            source_type: Source type (PROCESSOR, INPUT_PORT, OUTPUT_PORT).
            dest_type: Destination type (PROCESSOR, INPUT_PORT, OUTPUT_PORT).
            name: Optional connection label.
            back_pressure_count: Max queued FlowFiles before back pressure.
            back_pressure_size: Max queued data size before back pressure.
            flow_file_expiration: FlowFile expiration time.

        Returns:
            The ID of the created connection.
        """
        source_payload: dict[str, Any] = {
            "id": source_id,
            "groupId": pg_id,
            "type": source_type,
        }
        dest_payload: dict[str, Any] = {
            "id": dest_id,
            "groupId": pg_id,
            "type": dest_type,
        }

        payload: dict[str, Any] = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "source": source_payload,
                "destination": dest_payload,
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": back_pressure_count,
                "backPressureDataSizeThreshold": back_pressure_size,
                "flowFileExpiration": flow_file_expiration,
            },
        }

        result = self._api_post(f"/process-groups/{pg_id}/connections", payload)
        conn_id = result["id"]
        logger.info(
            "  Connected %s -[%s]-> %s",
            source_id[:8],
            ",".join(relationships),
            dest_id[:8],
        )
        return conn_id

    def start_processor(self, processor_id: str) -> None:
        """Start a single processor by ID.

        Args:
            processor_id: The processor ID to start.
        """
        proc_data = self._api_get(f"/processors/{processor_id}")
        revision = proc_data["revision"]

        payload = {
            "revision": revision,
            "status": {
                "runStatus": "RUNNING",
            },
            "component": {
                "id": processor_id,
                "state": "RUNNING",
            },
        }

        try:
            self._api_put(f"/processors/{processor_id}/run-status", payload)
            logger.info("Started processor %s", processor_id)
        except requests.HTTPError as exc:
            logger.warning("Failed to start processor %s: %s", processor_id, exc)

    def stop_process_group(self, pg_id: str) -> None:
        """Stop all components in a process group.

        Args:
            pg_id: The process group ID.
        """
        payload = {"id": pg_id, "state": "STOPPED"}
        try:
            self._api_put(f"/flow/process-groups/{pg_id}", payload)
            logger.info("Stopped process group %s", pg_id)
        except requests.HTTPError as exc:
            logger.warning("Failed to stop PG %s: %s", pg_id, exc)


# ---------------------------------------------------------------------------
# Step 1: Ensure MinIO parameters exist in the parameter context
# ---------------------------------------------------------------------------
def ensure_minio_parameters(
    client: NiFiClient, ctx_id: str, dry_run: bool = False
) -> None:
    """Add missing MinIO parameters to the existing parameter context.

    Reads the current parameter context, checks which MinIO parameters are
    already present, and adds any that are missing.

    Args:
        client: Authenticated NiFi API client.
        ctx_id: Parameter context ID.
        dry_run: If True, log what would happen without making changes.
    """
    logger.info("Checking parameter context %s for MinIO parameters...", ctx_id)

    ctx_data = client.get_parameter_context(ctx_id)
    existing_params: set[str] = set()

    for param_entry in ctx_data.get("component", {}).get("parameters", []):
        param = param_entry.get("parameter", {})
        existing_params.add(param.get("name", ""))

    missing_params = [
        p for p in MINIO_PARAMETERS if p["name"] not in existing_params
    ]

    if not missing_params:
        logger.info("All MinIO parameters already exist in context %s", ctx_id)
        return

    logger.info(
        "Adding %d missing MinIO parameter(s): %s",
        len(missing_params),
        [p["name"] for p in missing_params],
    )

    if dry_run:
        for param in missing_params:
            logger.info("  [DRY RUN] Would add parameter: %s = %s", param["name"], param["value"])
        return

    revision = ctx_data.get("revision", {"version": 0})
    client.update_parameter_context(ctx_id, revision, missing_params)


# ---------------------------------------------------------------------------
# Step 2: Ensure MinIO-S3-Credentials controller service exists
# ---------------------------------------------------------------------------
def ensure_minio_controller_service(
    client: NiFiClient, root_pg_id: str, dry_run: bool = False
) -> str | None:
    """Create and enable the MinIO-S3-Credentials controller service if missing.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID where the service is created.
        dry_run: If True, log what would happen without making changes.

    Returns:
        The service ID if created or already exists, None on dry-run skip.
    """
    service_name = "MinIO-S3-Credentials"
    service_type = (
        "org.apache.nifi.processors.aws.credentials.provider.service"
        ".AWSCredentialsProviderControllerService"
    )

    logger.info("Checking for existing '%s' controller service...", service_name)
    existing_services = client.get_controller_services(root_pg_id)

    if service_name in existing_services:
        svc_id = existing_services[service_name]
        logger.info(
            "Controller service '%s' already exists (ID: %s)", service_name, svc_id
        )
        return svc_id

    logger.info("Controller service '%s' not found, creating...", service_name)

    if dry_run:
        logger.info(
            "  [DRY RUN] Would create controller service '%s' of type %s",
            service_name,
            service_type,
        )
        return None

    svc_id = client.create_controller_service(
        process_group_id=root_pg_id,
        name=service_name,
        service_type=service_type,
        properties={
            "Access Key ID": "#{minio.access.key}",
            "Secret Access Key": "#{minio.secret.key}",
        },
        description="AWS credentials provider for MinIO S3-compatible object storage",
    )

    # Allow a moment for the service to be registered before enabling
    time.sleep(2)
    client.enable_controller_service(svc_id)
    return svc_id


# ---------------------------------------------------------------------------
# Step 3: Find PG-07 (Storage Layer / TimescaleDB) and create PutS3Object
# ---------------------------------------------------------------------------
def find_pg07_id(client: NiFiClient, root_pg_id: str) -> str | None:
    """Locate the PG-07 (TimescaleDB / Storage Layer) process group.

    Searches for a process group whose name contains 'TimescaleDB' or 'PG-07',
    first as a direct child of root, then inside any '3 - Storage Layer' parent.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID.

    Returns:
        The PG-07 NiFi ID if found, None otherwise.
    """
    search_terms = ["TimescaleDB", "PG-07"]

    # Try direct children of root first
    result = client.find_process_group(root_pg_id, search_terms)
    if result:
        return result[1]

    # Try inside a "Storage Layer" parent
    storage_parent = client.find_process_group(
        root_pg_id, ["Storage Layer", "3 -"]
    )
    if storage_parent:
        result = client.find_process_group(storage_parent[1], search_terms)
        if result:
            return result[1]

    # Fallback: recursive search from root
    result = client.find_process_group_recursive(root_pg_id, search_terms)
    if result:
        return result[1]

    return None


def find_pg10_id(client: NiFiClient, root_pg_id: str) -> str | None:
    """Locate the PG-10 (Dead Letter Queue / Error Handling) process group.

    Searches for a process group whose name contains 'Dead Letter' or 'PG-10',
    first as a direct child of root, then inside any '5 - Error Handling' parent.

    Args:
        client: Authenticated NiFi API client.
        root_pg_id: Root process group ID.

    Returns:
        The PG-10 NiFi ID if found, None otherwise.
    """
    search_terms = ["Dead Letter", "PG-10"]

    # Try direct children of root first
    result = client.find_process_group(root_pg_id, search_terms)
    if result:
        return result[1]

    # Try inside an "Error Handling" parent
    error_parent = client.find_process_group(
        root_pg_id, ["Error Handling", "5 -"]
    )
    if error_parent:
        result = client.find_process_group(error_parent[1], search_terms)
        if result:
            return result[1]

    # Fallback: recursive search from root
    result = client.find_process_group_recursive(root_pg_id, search_terms)
    if result:
        return result[1]

    return None


def find_source_processor(
    client: NiFiClient, pg_id: str
) -> dict[str, Any] | None:
    """Find a processor with available output connections to use as a data source.

    Looks for processors that have unconnected 'success' or 'original'
    relationships, preferring ConvertRecord or PrepareForDB processors.

    Args:
        client: Authenticated NiFi API client.
        pg_id: Process group ID to search.

    Returns:
        Processor dict if a suitable source is found, None otherwise.
    """
    processors = client.get_processors(pg_id)
    if not processors:
        logger.warning("No processors found in PG %s", pg_id)
        return None

    # Prefer processors named like data preparation stages
    preferred_names = ["PrepareForDB", "ConvertRecord", "AddDLQMetadata", "LogFailedRecord"]
    for proc in processors:
        comp = proc.get("component", {})
        proc_name = comp.get("name", "")
        for preferred in preferred_names:
            if preferred.lower() in proc_name.lower():
                logger.info(
                    "Found preferred source processor: '%s' (ID: %s)",
                    proc_name,
                    proc["id"],
                )
                return proc

    # Fallback: return the first processor
    first = processors[0]
    logger.info(
        "Using first available processor as source: '%s' (ID: %s)",
        first.get("component", {}).get("name", "unknown"),
        first["id"],
    )
    return first


def deploy_pg07_puts3(
    client: NiFiClient,
    pg_id: str,
    credentials_svc_id: str,
    dry_run: bool = False,
) -> str | None:
    """Create PutS3Object processor in PG-07 for MinIO data archival.

    Args:
        client: Authenticated NiFi API client.
        pg_id: PG-07 process group ID.
        credentials_svc_id: MinIO-S3-Credentials controller service ID.
        dry_run: If True, log what would happen without making changes.

    Returns:
        The created processor ID, or None on dry-run skip.
    """
    logger.info("Deploying PutS3Object (data archival) to PG-07 (%s)...", pg_id)

    # Check if a PutS3Object for data already exists
    existing = client.get_processors(pg_id)
    for proc in existing:
        comp = proc.get("component", {})
        proc_type = comp.get("type", "")
        proc_name = comp.get("name", "")
        if "PutS3Object" in proc_type and "MinIO" in proc_name:
            logger.info(
                "PutS3Object for MinIO already exists in PG-07: '%s' (ID: %s)",
                proc_name,
                proc["id"],
            )
            return proc["id"]

    if dry_run:
        logger.info("  [DRY RUN] Would create PutS3Object in PG-07 with:")
        logger.info("    Bucket: #{minio.bucket.data}")
        logger.info("    Object Key: sensors/${now():format('yyyy/MM/dd/HH')}/${UUID()}.json")
        logger.info("    Endpoint: #{minio.endpoint.url}")
        logger.info("    Credentials Service: %s", credentials_svc_id)
        return None

    # Stop PG-07 before adding processors
    client.stop_process_group(pg_id)
    time.sleep(2)

    put_s3 = client.create_processor(
        pg_id=pg_id,
        name="ArchiveToMinIO-SensorData",
        processor_type="org.apache.nifi.processors.aws.s3.PutS3Object",
        position={"x": -200, "y": 700},
        properties={
            "Bucket": "#{minio.bucket.data}",
            "Object Key": (
                "sensors/${now():format('yyyy/MM/dd/HH')}/${UUID()}.json"
            ),
            "Endpoint Override URL": "#{minio.endpoint.url}",
            "Region": "#{minio.region}",
            "AWS Credentials Provider service": credentials_svc_id,
            "Signer Override": "AWSS3V4SignerType",
            "Use Path Style Access": "true",
            "Content Type": "application/json",
            "Communications Timeout": "30 secs",
        },
        auto_terminated=["success"],
        comments=(
            "Archives enriched sensor readings to MinIO sensor-data bucket, "
            "partitioned by date/hour with UUID filenames"
        ),
    )

    proc_id = put_s3["id"]

    # Try to connect from an existing source processor
    source_proc = find_source_processor(client, pg_id)
    if source_proc:
        try:
            client.create_connection_within_pg(
                pg_id=pg_id,
                source_id=source_proc["id"],
                dest_id=proc_id,
                relationships=["success"],
                source_type="PROCESSOR",
                dest_type="PROCESSOR",
                name="to-minio-archive",
            )
            logger.info(
                "Connected '%s' -> ArchiveToMinIO-SensorData",
                source_proc.get("component", {}).get("name", "unknown"),
            )
        except requests.HTTPError as exc:
            logger.warning(
                "Could not auto-connect source to PutS3Object: %s "
                "(connection may need to be created manually)",
                exc,
            )

    return proc_id


# ---------------------------------------------------------------------------
# Step 4: Create PutS3Object in PG-10 (Dead Letter Queue)
# ---------------------------------------------------------------------------
def deploy_pg10_puts3(
    client: NiFiClient,
    pg_id: str,
    credentials_svc_id: str,
    dry_run: bool = False,
) -> str | None:
    """Create PutS3Object processor in PG-10 for MinIO DLQ archival.

    Args:
        client: Authenticated NiFi API client.
        pg_id: PG-10 process group ID.
        credentials_svc_id: MinIO-S3-Credentials controller service ID.
        dry_run: If True, log what would happen without making changes.

    Returns:
        The created processor ID, or None on dry-run skip.
    """
    logger.info("Deploying PutS3Object (DLQ archival) to PG-10 (%s)...", pg_id)

    # Check if a PutS3Object for DLQ already exists
    existing = client.get_processors(pg_id)
    for proc in existing:
        comp = proc.get("component", {})
        proc_type = comp.get("type", "")
        proc_name = comp.get("name", "")
        if "PutS3Object" in proc_type and ("DLQ" in proc_name or "dlq" in proc_name.lower()):
            logger.info(
                "PutS3Object for DLQ already exists in PG-10: '%s' (ID: %s)",
                proc_name,
                proc["id"],
            )
            return proc["id"]

    if dry_run:
        logger.info("  [DRY RUN] Would create PutS3Object in PG-10 with:")
        logger.info("    Bucket: #{minio.bucket.dlq}")
        logger.info("    Object Key: dlq/${now():format('yyyy/MM/dd')}/${UUID()}.json")
        logger.info("    Endpoint: #{minio.endpoint.url}")
        logger.info("    Credentials Service: %s", credentials_svc_id)
        return None

    # Stop PG-10 before adding processors
    client.stop_process_group(pg_id)
    time.sleep(2)

    put_s3 = client.create_processor(
        pg_id=pg_id,
        name="ArchiveToDLQBucket",
        processor_type="org.apache.nifi.processors.aws.s3.PutS3Object",
        position={"x": -200, "y": 1000},
        properties={
            "Bucket": "#{minio.bucket.dlq}",
            "Object Key": (
                "dlq/${now():format('yyyy/MM/dd')}/${UUID()}.json"
            ),
            "Endpoint Override URL": "#{minio.endpoint.url}",
            "Region": "#{minio.region}",
            "AWS Credentials Provider service": credentials_svc_id,
            "Signer Override": "AWSS3V4SignerType",
            "Use Path Style Access": "true",
            "Content Type": "application/json",
            "Communications Timeout": "30 secs",
        },
        auto_terminated=["success"],
        comments=(
            "Archives dead letter queue records to MinIO sensor-dlq bucket, "
            "partitioned by date with UUID filenames"
        ),
    )

    proc_id = put_s3["id"]

    # Try to connect from an existing source processor (e.g., LogFailedRecord)
    source_proc = find_source_processor(client, pg_id)
    if source_proc:
        try:
            client.create_connection_within_pg(
                pg_id=pg_id,
                source_id=source_proc["id"],
                dest_id=proc_id,
                relationships=["success"],
                source_type="PROCESSOR",
                dest_type="PROCESSOR",
                name="to-minio-dlq",
            )
            logger.info(
                "Connected '%s' -> ArchiveToDLQBucket",
                source_proc.get("component", {}).get("name", "unknown"),
            )
        except requests.HTTPError as exc:
            logger.warning(
                "Could not auto-connect source to PutS3Object (DLQ): %s "
                "(connection may need to be created manually)",
                exc,
            )

    return proc_id


# ---------------------------------------------------------------------------
# Step 5: Start the new processors
# ---------------------------------------------------------------------------
def start_processors(
    client: NiFiClient,
    processor_ids: list[str],
    dry_run: bool = False,
) -> None:
    """Start the newly created processors.

    Args:
        client: Authenticated NiFi API client.
        processor_ids: List of processor IDs to start.
        dry_run: If True, log what would happen without making changes.
    """
    if not processor_ids:
        logger.info("No new processors to start")
        return

    if dry_run:
        logger.info(
            "  [DRY RUN] Would start %d processor(s): %s",
            len(processor_ids),
            processor_ids,
        )
        return

    for proc_id in processor_ids:
        client.start_processor(proc_id)
        time.sleep(1)


# ---------------------------------------------------------------------------
# CLI and Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy MinIO S3 integration processors to a running NiFi instance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python deploy-minio-processors.py --username admin --password secret\n"
            "  python deploy-minio-processors.py --nifi-url https://nifi:8443 --username admin --password secret\n"
            "  python deploy-minio-processors.py --dry-run\n"
            "\n"
            "Environment Variables:\n"
            "  NIFI_URL       NiFi base URL (optional, overrides --nifi-url)\n"
            "  NIFI_USERNAME  NiFi admin username (optional, overrides --username)\n"
            "  NIFI_PASSWORD  NiFi admin password (optional, overrides --password)\n"
        ),
    )
    parser.add_argument(
        "--nifi-url",
        default=os.environ.get("NIFI_URL", "https://localhost:8443"),
        help="NiFi base URL (default: from NIFI_URL env or https://localhost:8443)",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="NiFi admin username (or set NIFI_USERNAME env var)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="NiFi admin password (or set NIFI_PASSWORD env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying NiFi (reads API but does not write)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Seconds to wait for NiFi readiness (default: 300)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    """Orchestrate the full MinIO S3 integration deployment."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    nifi_url = os.environ.get("NIFI_URL", args.nifi_url)
    username = os.environ.get("NIFI_USERNAME", args.username or "admin")
    password = os.environ.get("NIFI_PASSWORD", args.password or "")

    if nifi_url.startswith("https://") and (not username or not password):
        logger.error(
            "Username and password are required for HTTPS mode. "
            "Use --username/--password or set NIFI_USERNAME/NIFI_PASSWORD env vars."
        )
        sys.exit(1)

    dry_run = args.dry_run

    logger.info("=" * 70)
    logger.info("MinIO S3 Integration Deployment - NiFi Processors")
    logger.info("=" * 70)
    logger.info("NiFi URL:     %s", nifi_url)
    logger.info("Dry run:      %s", dry_run)
    logger.info("Root PG ID:   %s", ROOT_PG_ID)
    logger.info("Param Ctx ID: %s", PARAMETER_CONTEXT_ID)
    logger.info("=" * 70)

    client = NiFiClient(base_url=nifi_url, username=username, password=password)

    # ------------------------------------------------------------------
    # Step 1: Wait for NiFi readiness
    # ------------------------------------------------------------------
    logger.info("[Step 1/6] Waiting for NiFi to be ready...")
    client.wait_for_ready(timeout_seconds=args.timeout)

    # Verify root PG is accessible
    discovered_root = client.get_root_process_group_id()
    root_pg_id = ROOT_PG_ID if discovered_root == ROOT_PG_ID else discovered_root
    if discovered_root != ROOT_PG_ID:
        logger.warning(
            "Discovered root PG ID (%s) differs from expected (%s); using discovered ID",
            discovered_root,
            ROOT_PG_ID,
        )

    # ------------------------------------------------------------------
    # Step 2: Ensure MinIO parameters exist
    # ------------------------------------------------------------------
    logger.info("[Step 2/6] Ensuring MinIO parameters in parameter context...")
    try:
        ensure_minio_parameters(client, PARAMETER_CONTEXT_ID, dry_run=dry_run)
    except requests.HTTPError as exc:
        logger.error("Failed to update parameter context: %s", exc)
        logger.info("Continuing with deployment (parameters may already exist)...")

    # ------------------------------------------------------------------
    # Step 3: Ensure MinIO-S3-Credentials controller service
    # ------------------------------------------------------------------
    logger.info("[Step 3/6] Ensuring MinIO-S3-Credentials controller service...")
    credentials_svc_id = ensure_minio_controller_service(
        client, root_pg_id, dry_run=dry_run
    )

    if not credentials_svc_id and not dry_run:
        logger.error("Failed to create or find MinIO-S3-Credentials service")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 4: Deploy PutS3Object in PG-07 (Storage Layer)
    # ------------------------------------------------------------------
    logger.info("[Step 4/6] Deploying PutS3Object to PG-07 (Storage Layer)...")
    pg07_id = find_pg07_id(client, root_pg_id)
    pg07_proc_id: str | None = None

    if pg07_id:
        pg07_proc_id = deploy_pg07_puts3(
            client, pg07_id, credentials_svc_id or "DRY_RUN", dry_run=dry_run
        )
    else:
        logger.error(
            "Could not find PG-07 (TimescaleDB / Storage Layer) process group. "
            "Searched for names containing 'TimescaleDB' or 'PG-07'."
        )

    # ------------------------------------------------------------------
    # Step 5: Deploy PutS3Object in PG-10 (Dead Letter Queue)
    # ------------------------------------------------------------------
    logger.info("[Step 5/6] Deploying PutS3Object to PG-10 (Dead Letter Queue)...")
    pg10_id = find_pg10_id(client, root_pg_id)
    pg10_proc_id: str | None = None

    if pg10_id:
        pg10_proc_id = deploy_pg10_puts3(
            client, pg10_id, credentials_svc_id or "DRY_RUN", dry_run=dry_run
        )
    else:
        logger.error(
            "Could not find PG-10 (Dead Letter Queue / Error Handling) process group. "
            "Searched for names containing 'Dead Letter' or 'PG-10'."
        )

    # ------------------------------------------------------------------
    # Step 6: Start the new processors
    # ------------------------------------------------------------------
    logger.info("[Step 6/6] Starting new processors...")
    new_proc_ids = [pid for pid in [pg07_proc_id, pg10_proc_id] if pid]
    start_processors(client, new_proc_ids, dry_run=dry_run)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("MinIO S3 Deployment %s!", "preview complete" if dry_run else "complete")
    logger.info("  Parameter context: %s", PARAMETER_CONTEXT_ID)
    logger.info(
        "  Credentials service: %s",
        credentials_svc_id or "(dry-run, not created)",
    )
    logger.info("  PG-07 processor: %s", pg07_proc_id or "(not created)")
    logger.info("  PG-10 processor: %s", pg10_proc_id or "(not created)")
    logger.info("=" * 70)

    if not dry_run:
        logger.info("Open NiFi UI at %s/nifi/ to inspect the deployed processors", nifi_url)
    else:
        logger.info("Re-run without --dry-run to apply changes")


if __name__ == "__main__":
    main()
