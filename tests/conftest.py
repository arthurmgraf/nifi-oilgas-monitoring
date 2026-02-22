"""Shared fixtures for the NiFi Oil & Gas Monitoring Platform test suite.

Provides reusable fixtures for platforms, sensors, readings, and schema
paths that are used across unit and integration tests.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path manipulation so that ``import src.models...`` resolves correctly
# when running pytest from the project root.
# ---------------------------------------------------------------------------
_DATA_GENERATOR_SRC = str(
    Path(__file__).resolve().parent.parent / "data-generator" / "src"
)
if _DATA_GENERATOR_SRC not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data-generator"))

from src.models.equipment import Equipment
from src.models.platform import PLATFORMS, Platform
from src.models.reading import SensorReading
from src.models.sensor import Sensor, SensorType


# ---------------------------------------------------------------------------
# Platform fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_platform() -> Platform:
    """Return the ALPHA platform from the canonical registry."""
    return PLATFORMS["ALPHA"]


# ---------------------------------------------------------------------------
# Sensor fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_sensor() -> Sensor:
    """Return a temperature sensor attached to ALPHA's gas compressor."""
    return Sensor(
        sensor_id="ALPHA-COMP-S01",
        equipment_id="ALPHA-COMP",
        platform_id="ALPHA",
        sensor_type=SensorType.TEMPERATURE,
        unit="degC",
        min_range=50.0,
        max_range=250.0,
        subtype="discharge_temp",
    )


# ---------------------------------------------------------------------------
# Reading fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_reading() -> SensorReading:
    """Return a deterministic SensorReading with known field values."""
    return SensorReading(
        reading_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        platform_id="ALPHA",
        sensor_id="ALPHA-COMP-S01",
        sensor_type="TEMPERATURE",
        value=85.3,
        unit="degC",
        timestamp=1700000000000,
        quality_flag="GOOD",
    )


@pytest.fixture()
def sample_reading_dict() -> dict:
    """Return the expected dictionary representation of ``sample_reading``."""
    return {
        "reading_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "platform_id": "ALPHA",
        "sensor_id": "ALPHA-COMP-S01",
        "sensor_type": "TEMPERATURE",
        "value": 85.3,
        "unit": "degC",
        "timestamp": 1700000000000,
        "quality_flag": "GOOD",
    }


# ---------------------------------------------------------------------------
# Schema path fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def avro_schema_dir() -> Path:
    """Return the absolute path to the ``schemas/`` directory."""
    schema_dir = Path(__file__).resolve().parent.parent / "schemas"
    assert schema_dir.is_dir(), f"Schema directory not found: {schema_dir}"
    return schema_dir
