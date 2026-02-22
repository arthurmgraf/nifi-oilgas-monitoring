"""Unit tests for Avro schema validity and serialization round-trips.

Validates that all 6 ``.avsc`` schema files parse correctly with ``fastavro``,
have the required ``namespace`` field, and that a SensorReading can be
serialized and deserialized through the sensor-reading Avro schema.
"""

from __future__ import annotations

import datetime
import io
import json
from pathlib import Path

import fastavro
import pytest

from src.models.reading import SensorReading


# =========================================================================
# Helper to load and parse an Avro schema from a .avsc file
# =========================================================================


def _load_avro_schema(schema_path: Path) -> dict:
    """Load a .avsc file and return the parsed Avro schema dict."""
    raw = json.loads(schema_path.read_text(encoding="utf-8"))
    return fastavro.parse_schema(raw)


# =========================================================================
# Individual schema validity tests
# =========================================================================


class TestSchemaValidity:
    """Verify each .avsc file parses without errors."""

    def test_sensor_reading_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse sensor-reading.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "sensor-reading.avsc")
        assert schema is not None

    def test_sensor_validated_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse sensor-validated.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "sensor-validated.avsc")
        assert schema is not None

    def test_sensor_enriched_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse sensor-enriched.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "sensor-enriched.avsc")
        assert schema is not None

    def test_equipment_status_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse equipment-status.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "equipment-status.avsc")
        assert schema is not None

    def test_anomaly_alert_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse anomaly-alert.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "anomaly-alert.avsc")
        assert schema is not None

    def test_compliance_emission_schema_valid(self, avro_schema_dir: Path) -> None:
        """Load and parse compliance-emission.avsc with fastavro."""
        schema = _load_avro_schema(avro_schema_dir / "compliance-emission.avsc")
        assert schema is not None


# =========================================================================
# Namespace field validation
# =========================================================================


class TestSchemaNamespaces:
    """Verify all schemas have a ``namespace`` field."""

    SCHEMA_FILES = [
        "sensor-reading.avsc",
        "sensor-validated.avsc",
        "sensor-enriched.avsc",
        "equipment-status.avsc",
        "anomaly-alert.avsc",
        "compliance-emission.avsc",
    ]

    @pytest.mark.parametrize("schema_file", SCHEMA_FILES)
    def test_all_schemas_have_namespace(
        self, avro_schema_dir: Path, schema_file: str
    ) -> None:
        """Verify the raw JSON of each schema contains a ``namespace`` key."""
        raw = json.loads((avro_schema_dir / schema_file).read_text(encoding="utf-8"))
        assert "namespace" in raw, f"Schema {schema_file} is missing 'namespace' field"
        assert isinstance(raw["namespace"], str)
        assert len(raw["namespace"]) > 0


# =========================================================================
# Serialization round-trip test
# =========================================================================


class TestAvroRoundTrip:
    """Verify Avro serialization and deserialization of a SensorReading."""

    def test_reading_serialization(
        self, avro_schema_dir: Path, sample_reading: SensorReading
    ) -> None:
        """Create a SensorReading, serialize to Avro bytes, deserialize, verify round-trip."""
        schema_path = avro_schema_dir / "sensor-reading.avsc"
        raw_schema = json.loads(schema_path.read_text(encoding="utf-8"))
        parsed_schema = fastavro.parse_schema(raw_schema)

        # Build a record matching the Avro schema field expectations
        record = {
            "reading_id": sample_reading.reading_id,
            "platform_id": sample_reading.platform_id,
            "sensor_id": sample_reading.sensor_id,
            "sensor_type": sample_reading.sensor_type.lower(),  # enum uses lowercase
            "value": float(sample_reading.value),
            "unit": sample_reading.unit,
            "timestamp": sample_reading.timestamp,
            "quality_flag": sample_reading.quality_flag,
        }

        # Serialize to bytes
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed_schema, record)
        serialized_bytes = buffer.getvalue()

        assert len(serialized_bytes) > 0, "Serialized Avro bytes should not be empty"

        # Deserialize from bytes
        buffer.seek(0)
        deserialized = fastavro.schemaless_reader(buffer, parsed_schema)

        # Verify round-trip
        assert deserialized["reading_id"] == sample_reading.reading_id
        assert deserialized["platform_id"] == sample_reading.platform_id
        assert deserialized["sensor_id"] == sample_reading.sensor_id
        assert deserialized["sensor_type"] == sample_reading.sensor_type.lower()
        assert deserialized["value"] == pytest.approx(sample_reading.value)
        assert deserialized["unit"] == sample_reading.unit
        # fastavro deserializes timestamp-millis as datetime; convert back for comparison
        ts = deserialized["timestamp"]
        if isinstance(ts, datetime.datetime):
            ts_millis = int(ts.timestamp() * 1000)
        else:
            ts_millis = ts
        assert ts_millis == sample_reading.timestamp
        assert deserialized["quality_flag"] == sample_reading.quality_flag
