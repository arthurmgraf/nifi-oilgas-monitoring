"""NiFi Processor Bootstrap: Create processors inside each process group.

Populates the 10 process groups created by create-flow.py with actual NiFi
processors, forming the complete end-to-end data pipeline:

  MQTT -> Validation -> Enrichment -> Anomaly Detection -> Storage/Kafka/Alerts

Usage:
    python create-processors.py [--nifi-url http://localhost:8080] [--env-file .env.dev]

Prerequisites:
    - create-flow.py must have been run first (process groups, ports, connections exist)
    - Controller services must be created and enabled
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
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
logger = logging.getLogger("nifi-processors")


# ---------------------------------------------------------------------------
# NiFi REST API Client (reused from create-flow.py)
# ---------------------------------------------------------------------------
class NiFiClient:
    """Minimal NiFi REST API client for processor management."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _api_get(self, path: str) -> dict[str, Any]:
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def _api_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.post(url, json=payload)
        if resp.status_code not in (200, 201):
            logger.error("POST %s failed (HTTP %d): %s", path, resp.status_code, resp.text[:500])
            resp.raise_for_status()
        return resp.json()

    def _api_put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/nifi-api{path}"
        resp = self.session.put(url, json=payload)
        if resp.status_code not in (200, 201):
            logger.error("PUT %s failed (HTTP %d): %s", path, resp.status_code, resp.text[:500])
            resp.raise_for_status()
        return resp.json()

    def get_root_pg_id(self) -> str:
        data = self._api_get("/flow/process-groups/root")
        return data["processGroupFlow"]["id"]

    def get_child_process_groups(self, parent_id: str) -> dict[str, str]:
        """Return dict mapping PG name -> PG ID for all children."""
        data = self._api_get(f"/flow/process-groups/{parent_id}")
        pgs = data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
        result = {}
        for pg in pgs:
            comp = pg.get("component", {})
            result[comp["name"]] = comp["id"]
        return result

    def get_controller_services(self, pg_id: str) -> dict[str, str]:
        """Return dict mapping service name -> service ID."""
        data = self._api_get(f"/flow/process-groups/{pg_id}/controller-services")
        svcs = data.get("controllerServices", [])
        result = {}
        for svc in svcs:
            comp = svc.get("component", {})
            result[comp["name"]] = svc["id"]
        return result

    def get_ports(self, pg_id: str) -> dict[str, dict[str, str]]:
        """Return dict with 'input' and 'output' sub-dicts mapping port name -> port ID."""
        data = self._api_get(f"/process-groups/{pg_id}")
        result: dict[str, dict[str, str]] = {"input": {}, "output": {}}

        # Get input ports
        ip_data = self._api_get(f"/process-groups/{pg_id}/input-ports")
        for port in ip_data.get("inputPorts", []):
            comp = port.get("component", {})
            result["input"][comp["name"]] = comp["id"]

        # Get output ports
        op_data = self._api_get(f"/process-groups/{pg_id}/output-ports")
        for port in op_data.get("outputPorts", []):
            comp = port.get("component", {})
            result["output"][comp["name"]] = comp["id"]

        return result

    def get_processors(self, pg_id: str) -> list[dict[str, Any]]:
        """Return list of existing processors in a process group."""
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
        """Create a processor in the given process group."""
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
        logger.info("  Created processor '%s' (%s) -> %s", name, processor_type.split('.')[-1], proc_id)
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
        """Create a connection within a process group between processors/ports."""
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
        logger.info("  Connected %s -[%s]-> %s", source_id[:8], ",".join(relationships), dest_id[:8])
        return conn_id

    def start_process_group(self, pg_id: str) -> None:
        """Start all components in a process group."""
        payload = {"id": pg_id, "state": "RUNNING"}
        try:
            self._api_put(f"/flow/process-groups/{pg_id}", payload)
            logger.info("Started process group %s", pg_id)
        except requests.HTTPError as exc:
            logger.warning("Failed to start PG %s: %s", pg_id, exc)

    def stop_process_group(self, pg_id: str) -> None:
        """Stop all components in a process group."""
        payload = {"id": pg_id, "state": "STOPPED"}
        try:
            self._api_put(f"/flow/process-groups/{pg_id}", payload)
            logger.info("Stopped process group %s", pg_id)
        except requests.HTTPError as exc:
            logger.warning("Failed to stop PG %s: %s", pg_id, exc)


# ---------------------------------------------------------------------------
# Processor Definitions for Each Process Group
# ---------------------------------------------------------------------------

def build_pg01_mqtt_ingestion(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-01: MQTT Ingestion - ConsumeMQTT -> EvaluateJsonPath -> UpdateAttribute -> output."""
    logger.info("Building PG-01: MQTT Ingestion")

    # ConsumeMQTT: Subscribe to sensor data topics
    consume_mqtt = client.create_processor(
        pg_id=pg_id,
        name="ConsumeMQTT",
        processor_type="org.apache.nifi.processors.mqtt.ConsumeMQTT",
        position={"x": 0, "y": 400},
        properties={
            "Broker URI": "#{mqtt.broker.uri}",
            "Topic Filter": "#{mqtt.topic.pattern}",
            "Quality of Service(QoS)": "1",
            "Max Queue Size": "10000",
        },
        scheduling_period="0 sec",
        concurrency=2,
        comments="Subscribes to MQTT topics: sensors/+/+/data with QoS 1",
    )

    # EvaluateJsonPath: Extract key fields from JSON payload
    eval_json = client.create_processor(
        pg_id=pg_id,
        name="ExtractSensorFields",
        processor_type="org.apache.nifi.processors.standard.EvaluateJsonPath",
        position={"x": 0, "y": 700},
        properties={
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "sensor_id": "$.sensor_id",
            "platform_id": "$.platform_id",
            "sensor_type": "$.sensor_type",
            "timestamp": "$.timestamp",
            "value": "$.value",
            "unit": "$.unit",
        },
        auto_terminated=["failure", "unmatched"],
        comments="Extracts sensor_id, platform_id, sensor_type, timestamp, value, unit into FlowFile attributes",
    )

    # UpdateAttribute: Add ingestion metadata
    update_attr = client.create_processor(
        pg_id=pg_id,
        name="AddIngestionMetadata",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 0, "y": 1000},
        properties={
            "ingestion.source": "mqtt",
            "ingestion.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
            "pipeline.stage": "raw",
            "schema.name": "sensor-reading",
        },
        comments="Stamps each FlowFile with ingestion metadata for lineage tracking",
    )

    # Internal connections
    consume_id = consume_mqtt["id"]
    eval_id = eval_json["id"]
    update_id = update_attr["id"]
    output_port_id = ports["output"].get("validated-output")

    client.create_connection_within_pg(pg_id, consume_id, eval_id, ["Message"])
    client.create_connection_within_pg(pg_id, eval_id, update_id, ["matched"])
    if output_port_id:
        client.create_connection_within_pg(
            pg_id, update_id, output_port_id, ["success"],
            dest_type="OUTPUT_PORT",
        )


def build_pg02_schema_validation(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-02: Schema Validation - ValidateRecord + RouteOnAttribute for quality gates."""
    logger.info("Building PG-02: Schema Validation")

    input_port_id = ports["input"].get("input")

    # ValidateRecord: Validate against schema
    validate = client.create_processor(
        pg_id=pg_id,
        name="ValidateSchema",
        processor_type="org.apache.nifi.processors.standard.ValidateRecord",
        position={"x": 200, "y": 400},
        properties={
            "record-reader": svc_ids.get("JsonTreeReader", ""),
            "record-writer": svc_ids.get("JsonRecordSetWriter", ""),
            "Allow Extra Fields": "true",
            "Strict Type Checking": "false",
        },
        comments="Validates incoming sensor records against inferred JSON schema",
    )

    # RouteOnAttribute: Quality gate - check required fields present
    quality_gate = client.create_processor(
        pg_id=pg_id,
        name="QualityGate",
        processor_type="org.apache.nifi.processors.standard.RouteOnAttribute",
        position={"x": 200, "y": 700},
        properties={
            "Routing Strategy": "Route to 'matched' if all match",
            "has_sensor_id": "${sensor_id:isEmpty():not()}",
            "has_platform_id": "${platform_id:isEmpty():not()}",
            "has_value": "${value:isEmpty():not()}",
        },
        comments="Routes only records that have all required fields (sensor_id, platform_id, value)",
    )

    # UpdateAttribute: Mark as validated
    mark_valid = client.create_processor(
        pg_id=pg_id,
        name="MarkValidated",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 200, "y": 1000},
        properties={
            "pipeline.stage": "validated",
            "validation.status": "passed",
            "validation.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
        },
        comments="Marks FlowFile as having passed schema and quality validation",
    )

    # DuplicateFlowFile: Fan out to enrichment + kafka
    duplicate = client.create_processor(
        pg_id=pg_id,
        name="FanOutValidated",
        processor_type="org.apache.nifi.processors.standard.DuplicateFlowFile",
        position={"x": 200, "y": 1300},
        properties={
            "Number of Copies": "1",
        },
        comments="Duplicates validated FlowFile: original -> enrichment, copy -> kafka publishing",
    )

    validate_id = validate["id"]
    quality_id = quality_gate["id"]
    mark_id = mark_valid["id"]
    dup_id = duplicate["id"]

    enrichment_port = ports["output"].get("enrichment-output")
    kafka_port = ports["output"].get("kafka-output")
    failure_port = ports["output"].get("failure-output")

    # input port -> validate
    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, validate_id, [""],
            source_type="INPUT_PORT",
        )

    # validate -> quality gate (valid), validate -> DLQ (invalid)
    client.create_connection_within_pg(pg_id, validate_id, quality_id, ["valid"])
    if failure_port:
        client.create_connection_within_pg(
            pg_id, validate_id, failure_port, ["invalid"],
            dest_type="OUTPUT_PORT",
        )

    # quality gate -> mark validated (matched), quality gate -> DLQ (unmatched)
    client.create_connection_within_pg(pg_id, quality_id, mark_id, ["matched"])
    if failure_port:
        client.create_connection_within_pg(
            pg_id, quality_id, failure_port, ["unmatched"],
            dest_type="OUTPUT_PORT",
        )

    # mark validated -> duplicate
    client.create_connection_within_pg(pg_id, mark_id, dup_id, ["success"])

    # duplicate -> enrichment output (original) + kafka output (copy)
    if enrichment_port:
        client.create_connection_within_pg(
            pg_id, dup_id, enrichment_port, ["original"],
            dest_type="OUTPUT_PORT",
        )
    if kafka_port:
        client.create_connection_within_pg(
            pg_id, dup_id, kafka_port, ["duplicate"],
            dest_type="OUTPUT_PORT",
        )


def build_pg03_data_enrichment(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-03: Data Enrichment - LookupRecord against PostgreSQL for equipment metadata."""
    logger.info("Building PG-03: Data Enrichment")

    input_port_id = ports["input"].get("input")

    # LookupRecord: Enrich with equipment metadata from PostgreSQL
    lookup = client.create_processor(
        pg_id=pg_id,
        name="EnrichWithEquipmentData",
        processor_type="org.apache.nifi.processors.standard.LookupRecord",
        position={"x": 200, "y": 400},
        properties={
            "record-reader": svc_ids.get("JsonTreeReader", ""),
            "record-writer": svc_ids.get("JsonRecordSetWriter", ""),
            "lookup-service": svc_ids.get("PostgreSQL-RefData-DBCP", ""),
            "Result RecordPath": "/equipment_metadata",
            "Routing Strategy": "route-to-matched-unmatched",
        },
        auto_terminated=[],
        comments="Enriches sensor readings with equipment metadata from PostgreSQL reference data",
    )

    # UpdateAttribute: Add enrichment metadata
    enrich_meta = client.create_processor(
        pg_id=pg_id,
        name="AddEnrichmentMetadata",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 200, "y": 700},
        properties={
            "pipeline.stage": "enriched",
            "enrichment.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
            "enrichment.status": "complete",
        },
        comments="Marks FlowFile as enriched with equipment metadata",
    )

    # DuplicateFlowFile: Fan out to anomaly detection + timescaledb
    duplicate = client.create_processor(
        pg_id=pg_id,
        name="FanOutEnriched",
        processor_type="org.apache.nifi.processors.standard.DuplicateFlowFile",
        position={"x": 200, "y": 1000},
        properties={
            "Number of Copies": "1",
        },
        comments="Duplicates enriched FlowFile: original -> anomaly detection, copy -> timescaledb storage",
    )

    lookup_id = lookup["id"]
    enrich_id = enrich_meta["id"]
    dup_id = duplicate["id"]

    anomaly_port = ports["output"].get("anomaly-output")
    storage_port = ports["output"].get("storage-output")
    failure_port = ports["output"].get("failure-output")

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, lookup_id, [""],
            source_type="INPUT_PORT",
        )

    # lookup matched -> enrich metadata, unmatched also passes through (just without enrichment)
    client.create_connection_within_pg(pg_id, lookup_id, enrich_id, ["matched", "unmatched"])
    if failure_port:
        client.create_connection_within_pg(
            pg_id, lookup_id, failure_port, ["failure"],
            dest_type="OUTPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, enrich_id, dup_id, ["success"])

    if anomaly_port:
        client.create_connection_within_pg(
            pg_id, dup_id, anomaly_port, ["original"],
            dest_type="OUTPUT_PORT",
        )
    if storage_port:
        client.create_connection_within_pg(
            pg_id, dup_id, storage_port, ["duplicate"],
            dest_type="OUTPUT_PORT",
        )


def build_pg04_kafka_validated(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-04: Kafka Publishing (Validated) - Publishes validated sensor readings to Kafka."""
    logger.info("Building PG-04: Kafka Publishing (Validated)")

    input_port_id = ports["input"].get("input")

    # PublishKafka_2_6: Publish to sensor-readings-validated topic
    publish = client.create_processor(
        pg_id=pg_id,
        name="PublishToKafka-Validated",
        processor_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        position={"x": 200, "y": 400},
        properties={
            "bootstrap.servers": "#{kafka.bootstrap.servers}",
            "topic": "sensor-readings-validated",
            "Delivery Guarantee": "guarantee-replicated-delivery",
            "key": "${sensor_id}",
            "Message Demarcator": "\n",
            "Compression Type": "snappy",
            "max.block.ms": "5000",
        },
        auto_terminated=["success"],
        comments="Publishes validated sensor readings to Kafka topic 'sensor-readings-validated' with snappy compression",
    )

    # LogAttribute: Log failures for debugging
    log_failure = client.create_processor(
        pg_id=pg_id,
        name="LogKafkaFailure",
        processor_type="org.apache.nifi.processors.standard.LogAttribute",
        position={"x": 200, "y": 700},
        properties={
            "Log Level": "warn",
            "Log Payload": "false",
            "Attributes to Log": "kafka.*,sensor_id,platform_id",
        },
        auto_terminated=["success"],
        comments="Logs Kafka publishing failures for debugging",
    )

    publish_id = publish["id"]
    log_id = log_failure["id"]

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, publish_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, publish_id, log_id, ["failure"])


def build_pg05_anomaly_detection(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-05: Anomaly Detection - Threshold-based anomaly detection using RouteOnAttribute."""
    logger.info("Building PG-05: Anomaly Detection")

    input_port_id = ports["input"].get("input")

    # EvaluateJsonPath: Extract numeric value for threshold comparison
    eval_value = client.create_processor(
        pg_id=pg_id,
        name="ExtractNumericValue",
        processor_type="org.apache.nifi.processors.standard.EvaluateJsonPath",
        position={"x": 200, "y": 400},
        properties={
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "reading.value": "$.value",
            "reading.unit": "$.unit",
            "reading.quality": "$.quality_score",
        },
        auto_terminated=["failure", "unmatched"],
        comments="Extracts numeric value, unit, and quality score for threshold checks",
    )

    # RouteOnAttribute: Threshold-based anomaly detection
    threshold_check = client.create_processor(
        pg_id=pg_id,
        name="ThresholdAnomalyCheck",
        processor_type="org.apache.nifi.processors.standard.RouteOnAttribute",
        position={"x": 200, "y": 700},
        properties={
            "Routing Strategy": "Route to Property name",
            # Temperature anomaly (>150°C or <-40°C)
            "temperature_anomaly": "${sensor_type:equals('temperature'):and(${reading.value:gt(150):or(${reading.value:lt(-40)})})}",
            # Pressure anomaly (>5000 PSI or <0)
            "pressure_anomaly": "${sensor_type:equals('pressure'):and(${reading.value:gt(5000):or(${reading.value:lt(0)})})}",
            # Flow rate anomaly (>10000 bbl/d)
            "flow_anomaly": "${sensor_type:equals('flow_rate'):and(${reading.value:gt(10000)})}",
            # Low quality score (<0.5)
            "quality_anomaly": "${reading.quality:lt(0.5)}",
        },
        comments="Routes FlowFiles based on threshold anomalies for temperature, pressure, flow rate, and quality",
    )

    # UpdateAttribute: Mark anomalies with severity
    mark_anomaly = client.create_processor(
        pg_id=pg_id,
        name="MarkAnomaly",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": -200, "y": 1000},
        properties={
            "anomaly.detected": "true",
            "anomaly.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
            "anomaly.type": "${RouteOnAttribute.Route}",
            "anomaly.severity": "WARNING",
            "pipeline.stage": "anomaly-detected",
        },
        comments="Marks FlowFile with anomaly metadata including type and severity",
    )

    # UpdateAttribute: Mark normal readings
    mark_normal = client.create_processor(
        pg_id=pg_id,
        name="MarkNormal",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 600, "y": 1000},
        properties={
            "anomaly.detected": "false",
            "pipeline.stage": "analyzed",
        },
        auto_terminated=["success"],
        comments="Marks FlowFile as normal (no anomaly detected)",
    )

    eval_id = eval_value["id"]
    threshold_id = threshold_check["id"]
    anomaly_id = mark_anomaly["id"]
    normal_id = mark_normal["id"]
    alert_port = ports["output"].get("alert-output")
    failure_port = ports["output"].get("failure-output")

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, eval_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, eval_id, threshold_id, ["matched"])

    # All anomaly routes -> mark anomaly
    client.create_connection_within_pg(
        pg_id, threshold_id, anomaly_id,
        ["temperature_anomaly", "pressure_anomaly", "flow_anomaly", "quality_anomaly"],
    )

    # unmatched (normal) -> mark normal
    client.create_connection_within_pg(pg_id, threshold_id, normal_id, ["unmatched"])

    # anomaly -> alert output
    if alert_port:
        client.create_connection_within_pg(
            pg_id, anomaly_id, alert_port, ["success"],
            dest_type="OUTPUT_PORT",
        )

    if failure_port:
        client.create_connection_within_pg(
            pg_id, threshold_id, failure_port, ["failure"],
            dest_type="OUTPUT_PORT",
        )


def build_pg06_alert_routing(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-06: Alert Routing & Persistence - Route by severity and persist alerts."""
    logger.info("Building PG-06: Alert Routing & Persistence")

    input_port_id = ports["input"].get("input")

    # RouteOnAttribute: Route by anomaly severity
    severity_router = client.create_processor(
        pg_id=pg_id,
        name="RouteBySeverity",
        processor_type="org.apache.nifi.processors.standard.RouteOnAttribute",
        position={"x": 200, "y": 400},
        properties={
            "Routing Strategy": "Route to Property name",
            "critical": "${anomaly.severity:equals('CRITICAL')}",
            "warning": "${anomaly.severity:equals('WARNING')}",
            "info": "${anomaly.severity:equals('INFO')}",
        },
        comments="Routes anomaly alerts by severity level (CRITICAL, WARNING, INFO)",
    )

    # UpdateAttribute: Enrich critical alerts
    enrich_critical = client.create_processor(
        pg_id=pg_id,
        name="EnrichCriticalAlert",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": -200, "y": 700},
        properties={
            "alert.priority": "P1",
            "alert.notify": "true",
            "alert.channel": "pagerduty",
        },
        comments="Enriches critical alerts with P1 priority and notification routing",
    )

    # UpdateAttribute: Enrich warning alerts
    enrich_warning = client.create_processor(
        pg_id=pg_id,
        name="EnrichWarningAlert",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 200, "y": 700},
        properties={
            "alert.priority": "P2",
            "alert.notify": "true",
            "alert.channel": "slack",
        },
        comments="Enriches warning alerts with P2 priority and Slack notification",
    )

    # AttributesToJSON: Convert alert attributes to JSON for persistence
    attrs_to_json = client.create_processor(
        pg_id=pg_id,
        name="AlertToJSON",
        processor_type="org.apache.nifi.processors.standard.AttributesToJSON",
        position={"x": 200, "y": 1000},
        properties={
            "Destination": "flowfile-content",
            "Attributes List": "sensor_id,platform_id,sensor_type,value,unit,anomaly.type,anomaly.severity,anomaly.timestamp,alert.priority,alert.channel",
            "Include Core Attributes": "false",
        },
        comments="Serializes alert attributes to JSON payload for persistence and downstream consumers",
    )

    # DuplicateFlowFile: Fan out to kafka + compliance
    duplicate = client.create_processor(
        pg_id=pg_id,
        name="FanOutAlerts",
        processor_type="org.apache.nifi.processors.standard.DuplicateFlowFile",
        position={"x": 200, "y": 1300},
        properties={
            "Number of Copies": "1",
        },
        comments="Duplicates alert JSON: original -> Kafka anomaly topic, copy -> compliance reporting",
    )

    severity_id = severity_router["id"]
    critical_id = enrich_critical["id"]
    warning_id = enrich_warning["id"]
    json_id = attrs_to_json["id"]
    dup_id = duplicate["id"]

    kafka_port = ports["output"].get("kafka-output")
    compliance_port = ports["output"].get("compliance-output")
    failure_port = ports["output"].get("failure-output")

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, severity_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, severity_id, critical_id, ["critical"])
    client.create_connection_within_pg(pg_id, severity_id, warning_id, ["warning"])

    # info + unmatched -> directly to json (lower priority)
    client.create_connection_within_pg(pg_id, severity_id, json_id, ["info", "unmatched"])

    # critical/warning enriched -> json
    client.create_connection_within_pg(pg_id, critical_id, json_id, ["success"])
    client.create_connection_within_pg(pg_id, warning_id, json_id, ["success"])

    # json -> duplicate
    client.create_connection_within_pg(pg_id, json_id, dup_id, ["success"])
    if failure_port:
        client.create_connection_within_pg(
            pg_id, json_id, failure_port, ["failure"],
            dest_type="OUTPUT_PORT",
        )

    # duplicate -> kafka output (original) + compliance output (copy)
    if kafka_port:
        client.create_connection_within_pg(
            pg_id, dup_id, kafka_port, ["original"],
            dest_type="OUTPUT_PORT",
        )
    if compliance_port:
        client.create_connection_within_pg(
            pg_id, dup_id, compliance_port, ["duplicate"],
            dest_type="OUTPUT_PORT",
        )


def build_pg07_timescaledb_storage(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-07: TimescaleDB Storage - Write enriched sensor readings to TimescaleDB hypertables."""
    logger.info("Building PG-07: TimescaleDB Storage")

    input_port_id = ports["input"].get("input")

    # ConvertRecord: Ensure proper JSON format for PutDatabaseRecord
    convert = client.create_processor(
        pg_id=pg_id,
        name="PrepareForDB",
        processor_type="org.apache.nifi.processors.standard.ConvertRecord",
        position={"x": 200, "y": 400},
        properties={
            "record-reader": svc_ids.get("JsonTreeReader", ""),
            "record-writer": svc_ids.get("JsonRecordSetWriter", ""),
        },
        comments="Converts record format to ensure compatibility with PutDatabaseRecord",
    )

    # PutDatabaseRecord: Write to TimescaleDB sensor_readings hypertable
    put_db = client.create_processor(
        pg_id=pg_id,
        name="WriteToTimescaleDB",
        processor_type="org.apache.nifi.processors.standard.PutDatabaseRecord",
        position={"x": 200, "y": 700},
        properties={
            "record-reader": svc_ids.get("JsonTreeReader", ""),
            "Database Connection Pooling Service": svc_ids.get("TimescaleDB-SensorData-DBCP", ""),
            "Statement Type": "INSERT",
            "Schema Name": "public",
            "Table Name": "sensor_readings",
            "Translate Field Names": "true",
            "Unmatched Field Behavior": "Ignore Unmatched Fields",
            "Unmatched Column Behavior": "Ignore Unmatched Columns",
            "Quote Column Identifiers": "true",
            "Max Batch Size": "500",
        },
        auto_terminated=["success"],
        comments="Inserts enriched sensor readings into TimescaleDB sensor_readings hypertable in batches of 500",
    )

    # LogAttribute: Log DB write failures
    log_failure = client.create_processor(
        pg_id=pg_id,
        name="LogDBWriteFailure",
        processor_type="org.apache.nifi.processors.standard.LogAttribute",
        position={"x": 600, "y": 700},
        properties={
            "Log Level": "error",
            "Log Payload": "true",
            "Attributes to Log": "sensor_id,platform_id,PutDatabaseRecord.*",
        },
        auto_terminated=["success"],
        comments="Logs failed database write operations for debugging",
    )

    convert_id = convert["id"]
    put_db_id = put_db["id"]
    log_id = log_failure["id"]

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, convert_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, convert_id, put_db_id, ["success"])
    client.create_connection_within_pg(
        pg_id, convert_id, log_id, ["failure"],
    )

    client.create_connection_within_pg(pg_id, put_db_id, log_id, ["failure", "retry"])


def build_pg08_kafka_anomalies(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-08: Kafka Publishing (Anomalies) - Publish anomaly alerts to Kafka."""
    logger.info("Building PG-08: Kafka Publishing (Anomalies)")

    input_port_id = ports["input"].get("input")

    # PublishKafka_2_6: Publish to anomaly-alerts topic
    publish = client.create_processor(
        pg_id=pg_id,
        name="PublishToKafka-Anomalies",
        processor_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        position={"x": 200, "y": 400},
        properties={
            "bootstrap.servers": "#{kafka.bootstrap.servers}",
            "topic": "anomaly-alerts",
            "Delivery Guarantee": "guarantee-replicated-delivery",
            "key": "${sensor_id}-${anomaly.type}",
            "Message Demarcator": "\n",
            "Compression Type": "snappy",
            "max.block.ms": "5000",
        },
        auto_terminated=["success"],
        comments="Publishes anomaly alerts to Kafka topic 'anomaly-alerts' with snappy compression",
    )

    # LogAttribute: Log failures
    log_failure = client.create_processor(
        pg_id=pg_id,
        name="LogKafkaAnomalyFailure",
        processor_type="org.apache.nifi.processors.standard.LogAttribute",
        position={"x": 200, "y": 700},
        properties={
            "Log Level": "error",
            "Log Payload": "false",
            "Attributes to Log": "kafka.*,sensor_id,anomaly.*",
        },
        auto_terminated=["success"],
        comments="Logs Kafka anomaly publishing failures",
    )

    publish_id = publish["id"]
    log_id = log_failure["id"]

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, publish_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, publish_id, log_id, ["failure"])


def build_pg09_compliance(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-09: Compliance & Reporting - Filter and publish compliance events."""
    logger.info("Building PG-09: Compliance & Reporting")

    input_port_id = ports["input"].get("input")

    # RouteOnAttribute: Filter for compliance-relevant events
    compliance_filter = client.create_processor(
        pg_id=pg_id,
        name="FilterComplianceEvents",
        processor_type="org.apache.nifi.processors.standard.RouteOnAttribute",
        position={"x": 200, "y": 400},
        properties={
            "Routing Strategy": "Route to Property name",
            "emission_event": "${sensor_type:equals('gas_emission'):or(${sensor_type:equals('h2s_concentration')})}",
            "safety_event": "${anomaly.severity:equals('CRITICAL')}",
        },
        auto_terminated=["unmatched"],
        comments="Filters events for regulatory compliance: emissions and safety-critical anomalies",
    )

    # UpdateAttribute: Add compliance metadata
    compliance_meta = client.create_processor(
        pg_id=pg_id,
        name="AddComplianceMetadata",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 200, "y": 700},
        properties={
            "compliance.report.type": "${RouteOnAttribute.Route}",
            "compliance.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
            "compliance.regulatory.framework": "EPA-40CFR98",
            "pipeline.stage": "compliance",
        },
        comments="Adds regulatory compliance metadata (EPA 40 CFR 98 framework)",
    )

    # PublishKafka_2_6: Publish to compliance-events topic
    publish = client.create_processor(
        pg_id=pg_id,
        name="PublishToKafka-Compliance",
        processor_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        position={"x": 200, "y": 1000},
        properties={
            "bootstrap.servers": "#{kafka.bootstrap.servers}",
            "topic": "compliance-events",
            "Delivery Guarantee": "guarantee-replicated-delivery",
            "key": "${platform_id}-${compliance.report.type}",
            "Compression Type": "snappy",
        },
        auto_terminated=["success"],
        comments="Publishes compliance events to Kafka topic 'compliance-events'",
    )

    # LogAttribute: Log failures
    log_failure = client.create_processor(
        pg_id=pg_id,
        name="LogComplianceFailure",
        processor_type="org.apache.nifi.processors.standard.LogAttribute",
        position={"x": 600, "y": 1000},
        properties={
            "Log Level": "error",
            "Log Payload": "true",
        },
        auto_terminated=["success"],
        comments="Logs compliance event publishing failures (payload included for audit trail)",
    )

    filter_id = compliance_filter["id"]
    meta_id = compliance_meta["id"]
    publish_id = publish["id"]
    log_id = log_failure["id"]

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, filter_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(
        pg_id, filter_id, meta_id, ["emission_event", "safety_event"],
    )

    client.create_connection_within_pg(pg_id, meta_id, publish_id, ["success"])
    client.create_connection_within_pg(pg_id, publish_id, log_id, ["failure"])


def build_pg10_dead_letter_queue(
    client: NiFiClient, pg_id: str, ports: dict, svc_ids: dict
) -> None:
    """PG-10: Dead Letter Queue - Capture, log, and archive failed records."""
    logger.info("Building PG-10: Dead Letter Queue")

    input_port_id = ports["input"].get("input")

    # UpdateAttribute: Add DLQ metadata
    dlq_meta = client.create_processor(
        pg_id=pg_id,
        name="AddDLQMetadata",
        processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
        position={"x": 200, "y": 400},
        properties={
            "dlq.timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
            "dlq.source.stage": "${pipeline.stage}",
            "dlq.original.filename": "${filename}",
            "pipeline.stage": "dead-letter-queue",
        },
        comments="Stamps failed records with DLQ metadata for debugging and reprocessing",
    )

    # LogAttribute: Log the failed record details
    log_dlq = client.create_processor(
        pg_id=pg_id,
        name="LogFailedRecord",
        processor_type="org.apache.nifi.processors.standard.LogAttribute",
        position={"x": 200, "y": 700},
        properties={
            "Log Level": "warn",
            "Log Payload": "true",
            "Attributes to Log": "sensor_id,platform_id,pipeline.stage,dlq.*,validation.*,anomaly.*",
        },
        comments="Logs failed record attributes and payload for debugging",
    )

    # PutFile: Write failed records to disk for manual review
    put_file = client.create_processor(
        pg_id=pg_id,
        name="ArchiveToDisk",
        processor_type="org.apache.nifi.processors.standard.PutFile",
        position={"x": 200, "y": 1000},
        properties={
            "Directory": "/data/import/dlq/${now():format('yyyy-MM-dd')}",
            "Conflict Resolution Strategy": "replace",
            "Create Missing Directories": "true",
        },
        auto_terminated=["success", "failure"],
        comments="Archives failed records to /data/import/dlq/ partitioned by date for manual review",
    )

    # PublishKafka_2_6: Also publish to DLQ Kafka topic for monitoring
    publish_dlq = client.create_processor(
        pg_id=pg_id,
        name="PublishToKafka-DLQ",
        processor_type="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        position={"x": 600, "y": 700},
        properties={
            "bootstrap.servers": "#{kafka.bootstrap.servers}",
            "topic": "sensor-readings-dlq",
            "Delivery Guarantee": "guarantee-replicated-delivery",
            "key": "${sensor_id}",
        },
        auto_terminated=["success", "failure"],
        comments="Publishes failed records to Kafka DLQ topic for monitoring dashboards",
    )

    meta_id = dlq_meta["id"]
    log_id = log_dlq["id"]
    file_id = put_file["id"]
    dlq_id = publish_dlq["id"]

    if input_port_id:
        client.create_connection_within_pg(
            pg_id, input_port_id, meta_id, [""],
            source_type="INPUT_PORT",
        )

    client.create_connection_within_pg(pg_id, meta_id, log_id, ["success"])

    # After logging, fan out to disk archive + kafka DLQ
    client.create_connection_within_pg(pg_id, log_id, file_id, ["success"])
    client.create_connection_within_pg(pg_id, log_id, dlq_id, ["success"])


# ---------------------------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------------------------

PG_BUILDERS = {
    "PG-01: MQTT Ingestion": build_pg01_mqtt_ingestion,
    "PG-02: Schema Validation": build_pg02_schema_validation,
    "PG-03: Data Enrichment": build_pg03_data_enrichment,
    "PG-04: Kafka Publishing (Validated)": build_pg04_kafka_validated,
    "PG-05: Anomaly Detection": build_pg05_anomaly_detection,
    "PG-06: Alert Routing & Persistence": build_pg06_alert_routing,
    "PG-07: TimescaleDB Storage": build_pg07_timescaledb_storage,
    "PG-08: Kafka Publishing (Anomalies)": build_pg08_kafka_anomalies,
    "PG-09: Compliance & Reporting": build_pg09_compliance,
    "PG-10: Dead Letter Queue": build_pg10_dead_letter_queue,
}


def load_env_file(filepath: str) -> None:
    """Load environment variables from a .env file."""
    path = Path(filepath)
    if not path.exists():
        logger.warning("Env file not found: %s", filepath)
        return
    loaded = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
                    loaded += 1
    logger.info("Loaded %d environment variables from %s", loaded, filepath)


def main() -> None:
    """Orchestrate processor creation across all process groups."""
    parser = argparse.ArgumentParser(
        description="Create NiFi processors inside process groups for end-to-end data flow",
    )
    parser.add_argument(
        "--nifi-url", default="http://localhost:8080",
        help="NiFi base URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--env-file", default=None,
        help="Path to .env file for variable substitution",
    )
    parser.add_argument(
        "--start", action="store_true",
        help="Start all process groups after creating processors",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--pg", default=None,
        help="Only build processors for a specific PG (e.g., 'PG-01')",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.env_file:
        load_env_file(args.env_file)

    nifi_url = os.environ.get("NIFI_URL", args.nifi_url)
    client = NiFiClient(base_url=nifi_url)

    logger.info("=" * 70)
    logger.info("NiFi Processor Bootstrap - Oil & Gas Monitoring Platform")
    logger.info("=" * 70)
    logger.info("NiFi URL: %s", nifi_url)

    # Get root PG and discover child PGs
    root_pg_id = client.get_root_pg_id()
    logger.info("Root PG ID: %s", root_pg_id)

    child_pgs = client.get_child_process_groups(root_pg_id)
    logger.info("Found %d process groups", len(child_pgs))

    # Get controller service IDs
    svc_ids = client.get_controller_services(root_pg_id)
    logger.info("Found %d controller services: %s", len(svc_ids), list(svc_ids.keys()))

    # Build processors for each PG
    built = 0
    errors = 0

    for pg_name, builder_fn in PG_BUILDERS.items():
        # Filter if --pg specified
        if args.pg and not pg_name.startswith(args.pg):
            continue

        pg_id = child_pgs.get(pg_name)
        if not pg_id:
            logger.error("Process group '%s' not found in NiFi", pg_name)
            errors += 1
            continue

        # Check if PG already has processors
        existing = client.get_processors(pg_id)
        if existing:
            logger.warning(
                "PG '%s' already has %d processors - skipping (delete manually to rebuild)",
                pg_name, len(existing),
            )
            continue

        # Get ports for this PG
        ports = client.get_ports(pg_id)

        try:
            # Stop PG before adding processors
            client.stop_process_group(pg_id)
            builder_fn(client, pg_id, ports, svc_ids)
            built += 1
            logger.info("Successfully built processors for '%s'", pg_name)
        except Exception:
            logger.exception("Failed to build processors for '%s'", pg_name)
            errors += 1

    # Start all PGs if requested
    if args.start:
        logger.info("Starting all process groups...")
        for pg_name, pg_id in child_pgs.items():
            if args.pg and not pg_name.startswith(args.pg):
                continue
            client.start_process_group(pg_id)

    logger.info("=" * 70)
    logger.info("Processor bootstrap complete!")
    logger.info("  Built: %d process groups", built)
    logger.info("  Errors: %d", errors)
    logger.info("=" * 70)
    logger.info("Open NiFi UI at %s/nifi/ to inspect processors", nifi_url)


if __name__ == "__main__":
    main()
