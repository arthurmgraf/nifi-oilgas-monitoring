"""NiFi Bootstrap: Enable PrometheusReportingTask for Grafana metrics.

Configures NiFi's built-in PrometheusReportingTask to expose metrics
at port 9093, which Prometheus scrapes for the Pipeline Health dashboard.

Usage:
    python enable-prometheus-reporting.py [--nifi-url http://localhost:8080]

Prerequisites:
    - NiFi must be running and accessible
    - requests library: pip install requests
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nifi-prometheus")

PROMETHEUS_PORT = "9093"


def wait_for_nifi(base_url: str, timeout: int = 300) -> bool:
    """Wait for NiFi to be ready."""
    logger.info("Waiting for NiFi at %s ...", base_url)
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(
                f"{base_url}/nifi-api/system-diagnostics",
                verify=False,
                timeout=10,
            )
            if resp.status_code == 200:
                logger.info("NiFi is ready.")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(5)
    logger.error("NiFi did not become ready within %ds", timeout)
    return False


def get_existing_reporting_tasks(base_url: str) -> list[dict]:
    """Get all existing reporting tasks."""
    resp = requests.get(
        f"{base_url}/nifi-api/flow/reporting-tasks",
        verify=False,
    )
    resp.raise_for_status()
    return resp.json().get("reportingTasks", [])


def create_prometheus_reporting_task(base_url: str) -> str | None:
    """Create and start the PrometheusReportingTask.

    Returns the reporting task ID or None if already exists.
    """
    # Check if already exists
    existing = get_existing_reporting_tasks(base_url)
    for task in existing:
        comp = task.get("component", {})
        if "Prometheus" in comp.get("type", ""):
            task_id = comp["id"]
            state = comp.get("state", "UNKNOWN")
            logger.info(
                "PrometheusReportingTask already exists (id=%s, state=%s)",
                task_id,
                state,
            )
            if state != "RUNNING":
                _start_reporting_task(base_url, task_id, task["revision"])
            return task_id

    # Create the reporting task
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": "PrometheusReportingTask",
            "type": "org.apache.nifi.reporting.prometheus.PrometheusReportingTask",
            "properties": {
                "prometheus-reporting-task-metrics-endpoint-port": PROMETHEUS_PORT,
                "prometheus-reporting-task-metrics-send-jvm": "true",
            },
            "comments": "Exposes NiFi metrics for Prometheus/Grafana on port "
            + PROMETHEUS_PORT,
            "schedulingStrategy": "TIMER_DRIVEN",
            "schedulingPeriod": "30 sec",
        },
    }

    logger.info("Creating PrometheusReportingTask on port %s ...", PROMETHEUS_PORT)
    resp = requests.post(
        f"{base_url}/nifi-api/controller/reporting-tasks",
        json=payload,
        verify=False,
    )

    if resp.status_code not in (200, 201):
        logger.error("Failed to create reporting task: %s", resp.text[:500])
        resp.raise_for_status()

    result = resp.json()
    task_id = result["id"]
    revision = result["revision"]
    logger.info("Created PrometheusReportingTask (id=%s)", task_id)

    # Start the reporting task
    _start_reporting_task(base_url, task_id, revision)

    return task_id


def _start_reporting_task(
    base_url: str, task_id: str, revision: dict
) -> None:
    """Start a reporting task."""
    payload = {
        "revision": revision,
        "state": "RUNNING",
    }
    resp = requests.put(
        f"{base_url}/nifi-api/reporting-tasks/{task_id}/run-status",
        json=payload,
        verify=False,
    )
    if resp.status_code == 200:
        logger.info("PrometheusReportingTask started successfully.")
    else:
        logger.warning(
            "Failed to start reporting task (HTTP %d): %s",
            resp.status_code,
            resp.text[:300],
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enable NiFi PrometheusReportingTask for Grafana metrics",
    )
    parser.add_argument(
        "--nifi-url",
        default=os.environ.get("NIFI_URL", "http://localhost:8080"),
        help="NiFi base URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip waiting for NiFi readiness",
    )
    args = parser.parse_args()

    nifi_url = args.nifi_url.rstrip("/")

    logger.info("=" * 60)
    logger.info("NiFi PrometheusReportingTask Bootstrap")
    logger.info("=" * 60)
    logger.info("NiFi URL: %s", nifi_url)
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)

    if not args.no_wait:
        if not wait_for_nifi(nifi_url):
            sys.exit(1)

    task_id = create_prometheus_reporting_task(nifi_url)

    if task_id:
        logger.info("=" * 60)
        logger.info("PrometheusReportingTask is active!")
        logger.info("  Metrics URL: http://nifi:%s/metrics", PROMETHEUS_PORT)
        logger.info("  Prometheus scrape target: nifi:%s", PROMETHEUS_PORT)
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
