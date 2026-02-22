"""End-to-end integration tests for the full NiFi pipeline.

These tests publish messages into the pipeline entry points (MQTT) and verify
that data flows through NiFi processors into the downstream sinks (TimescaleDB).
They require the entire Docker Compose stack to be running.

Run with:  ``pytest -m e2e tests/integration/test_end_to_end.py``

WARNING: These tests have long timeouts (up to 30 seconds) to allow NiFi
processors and Kafka consumers to propagate messages end-to-end.
"""

from __future__ import annotations

import json
import time
import uuid

import paho.mqtt.client as mqtt
import psycopg2
import pytest


# =========================================================================
# Constants
# =========================================================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "oilgas/sensors/ALPHA"

TIMESCALEDB_DSN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "oilgas_monitoring",
    "user": "oilgas",
    "password": "oilgas_password",
}

# Maximum time (seconds) to wait for end-to-end propagation
E2E_TIMEOUT = 15
E2E_POLL_INTERVAL = 1


# =========================================================================
# Helpers
# =========================================================================


def _publish_mqtt_message(topic: str, payload: dict) -> None:
    """Publish a single JSON payload to the MQTT broker."""
    client = mqtt.Client(client_id=f"e2e-test-{uuid.uuid4().hex[:8]}")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    client.publish(topic, json.dumps(payload), qos=1)
    client.disconnect()


def _query_timescaledb(query: str, params: tuple = ()) -> list:
    """Execute a query against TimescaleDB and return all rows."""
    conn = psycopg2.connect(**TIMESCALEDB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


def _wait_for_row(query: str, params: tuple, timeout: int = E2E_TIMEOUT) -> list:
    """Poll TimescaleDB until the query returns at least one row or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = _query_timescaledb(query, params)
        if rows:
            return rows
        time.sleep(E2E_POLL_INTERVAL)
    return []


# =========================================================================
# E2E: MQTT -> NiFi -> Kafka -> TimescaleDB
# =========================================================================


@pytest.mark.e2e
class TestMqttToTimescaleDB:
    """Verify full pipeline from MQTT ingestion to TimescaleDB persistence."""

    def test_mqtt_to_timescaledb(self) -> None:
        """Publish an MQTT message, wait, then query TimescaleDB for the reading."""
        reading_id = str(uuid.uuid4())
        timestamp_ms = int(time.time() * 1000)

        payload = {
            "reading_id": reading_id,
            "platform_id": "ALPHA",
            "sensor_id": "ALPHA-COMP-S01",
            "sensor_type": "TEMPERATURE",
            "value": 87.5,
            "unit": "degC",
            "timestamp": timestamp_ms,
            "quality_flag": "GOOD",
        }

        _publish_mqtt_message(MQTT_TOPIC, payload)

        # Poll TimescaleDB for the reading
        query = "SELECT reading_id, value FROM sensor_readings WHERE reading_id = %s"
        rows = _wait_for_row(query, (reading_id,))

        assert len(rows) >= 1, (
            f"Reading {reading_id} not found in TimescaleDB after {E2E_TIMEOUT}s"
        )
        assert rows[0][0] == reading_id
        assert float(rows[0][1]) == pytest.approx(87.5)


# =========================================================================
# E2E: Anomaly detection
# =========================================================================


@pytest.mark.e2e
class TestAnomalyDetection:
    """Verify that readings above threshold trigger anomaly alerts."""

    def test_anomaly_generates_alert(self) -> None:
        """Publish a reading above threshold, wait, query alert_history."""
        reading_id = str(uuid.uuid4())
        timestamp_ms = int(time.time() * 1000)

        # Extremely high temperature to trigger anomaly detection
        payload = {
            "reading_id": reading_id,
            "platform_id": "ALPHA",
            "sensor_id": "ALPHA-COMP-S01",
            "sensor_type": "TEMPERATURE",
            "value": 999.9,
            "unit": "degC",
            "timestamp": timestamp_ms,
            "quality_flag": "GOOD",
        }

        _publish_mqtt_message(MQTT_TOPIC, payload)

        # Poll for an alert linked to this reading
        query = (
            "SELECT alert_id, reading_id, severity "
            "FROM alert_history "
            "WHERE reading_id = %s"
        )
        rows = _wait_for_row(query, (reading_id,))

        assert len(rows) >= 1, (
            f"No alert found for reading {reading_id} after {E2E_TIMEOUT}s"
        )
        # Alert should be CRITICAL or WARNING given the extreme value
        severity = rows[0][2]
        assert severity in ("CRITICAL", "WARNING"), (
            f"Expected CRITICAL or WARNING severity, got {severity}"
        )
