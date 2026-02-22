"""Integration tests for the NiFi Oil & Gas Monitoring stack health.

These tests verify that all infrastructure components (NiFi, Kafka, Schema
Registry, TimescaleDB, PostgreSQL) are running and properly configured. They
require the full Docker Compose stack to be up.

Run with:  ``pytest -m integration tests/integration/test_nifi_health.py``
"""

from __future__ import annotations

import urllib3
import pytest
import requests
import psycopg2

# Suppress insecure-request warnings for NiFi self-signed TLS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =========================================================================
# Constants
# =========================================================================

NIFI_BASE_URL = "https://localhost:8443"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TIMESCALEDB_DSN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "oilgas_monitoring",
    "user": "oilgas",
    "password": "oilgas_password",
}
POSTGRES_DSN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "oilgas_metadata",
    "user": "oilgas",
    "password": "oilgas_password",
}

EXPECTED_KAFKA_TOPICS = {
    "sensor-raw",
    "sensor-validated",
    "sensor-enriched",
    "anomaly-alerts",
    "equipment-status",
    "compliance-emissions",
    "dlq-sensor-raw",
}

EXPECTED_SCHEMA_SUBJECTS = {
    "sensor-raw-value",
    "sensor-validated-value",
    "sensor-enriched-value",
    "anomaly-alerts-value",
    "equipment-status-value",
    "compliance-emissions-value",
}


# =========================================================================
# NiFi API tests
# =========================================================================


@pytest.mark.integration
class TestNiFiApi:
    """Verify the NiFi REST API is reachable and functional."""

    def test_nifi_api_reachable(self) -> None:
        """GET /nifi-api/system-diagnostics should return 200 OK."""
        url = f"{NIFI_BASE_URL}/nifi-api/system-diagnostics"
        response = requests.get(url, verify=False, timeout=10)
        assert response.status_code == 200, (
            f"NiFi API returned {response.status_code}: {response.text[:200]}"
        )

    def test_nifi_process_groups(self) -> None:
        """GET /nifi-api/flow/process-groups/root should contain child groups."""
        url = f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/root"
        response = requests.get(url, verify=False, timeout=10)
        assert response.status_code == 200

        data = response.json()
        process_group_flow = data.get("processGroupFlow", {})
        flow = process_group_flow.get("flow", {})
        process_groups = flow.get("processGroups", [])

        assert len(process_groups) > 0, (
            "Root process group should have at least one child process group"
        )


# =========================================================================
# Kafka tests
# =========================================================================


@pytest.mark.integration
class TestKafka:
    """Verify Kafka topics are created and accessible."""

    def test_kafka_topics_exist(self) -> None:
        """List Kafka topics via kafka-python and verify all 7 expected topics."""
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            consumer_timeout_ms=5000,
        )
        try:
            topics = consumer.topics()
            for expected in EXPECTED_KAFKA_TOPICS:
                assert expected in topics, f"Missing Kafka topic: {expected}"
        finally:
            consumer.close()


# =========================================================================
# Schema Registry tests
# =========================================================================


@pytest.mark.integration
class TestSchemaRegistry:
    """Verify the Confluent Schema Registry has expected subjects."""

    def test_schema_registry_subjects(self) -> None:
        """GET /subjects should return the 6 expected Avro subjects."""
        url = f"{SCHEMA_REGISTRY_URL}/subjects"
        response = requests.get(url, timeout=10)
        assert response.status_code == 200, (
            f"Schema Registry returned {response.status_code}"
        )

        subjects = set(response.json())
        for expected in EXPECTED_SCHEMA_SUBJECTS:
            assert expected in subjects, (
                f"Missing Schema Registry subject: {expected}"
            )


# =========================================================================
# TimescaleDB tests
# =========================================================================


@pytest.mark.integration
class TestTimescaleDB:
    """Verify TimescaleDB is running and has the expected tables."""

    def test_timescaledb_connection(self) -> None:
        """Connect to TimescaleDB on port 5433, verify sensor_readings table exists."""
        conn = psycopg2.connect(**TIMESCALEDB_DSN)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'sensor_readings'
                    );
                    """
                )
                exists = cur.fetchone()[0]
                assert exists is True, (
                    "Table 'sensor_readings' not found in TimescaleDB"
                )
        finally:
            conn.close()


# =========================================================================
# PostgreSQL (metadata) tests
# =========================================================================


@pytest.mark.integration
class TestPostgres:
    """Verify the PostgreSQL metadata database is running and seeded."""

    def test_postgres_connection(self) -> None:
        """Connect to PostgreSQL on port 5432, verify platforms table has 5 rows."""
        conn = psycopg2.connect(**POSTGRES_DSN)
        try:
            with conn.cursor() as cur:
                # Verify table exists
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'platforms'
                    );
                    """
                )
                exists = cur.fetchone()[0]
                assert exists is True, "Table 'platforms' not found in PostgreSQL"

                # Verify row count
                cur.execute("SELECT COUNT(*) FROM platforms;")
                count = cur.fetchone()[0]
                assert count == 5, (
                    f"Expected 5 rows in platforms table, found {count}"
                )
        finally:
            conn.close()
