"""Unit tests for all domain models: Platform, Sensor, Equipment, SensorReading.

Validates creation, field access, serialization, immutability, and completeness
of the canonical registries (PLATFORMS, EQUIPMENT).
"""

from __future__ import annotations

import json

import pytest

from src.models.equipment import EQUIPMENT, Equipment, get_equipment_for_platform
from src.models.platform import PLATFORMS, Platform
from src.models.reading import SensorReading
from src.models.sensor import Sensor, SensorType, generate_sensors_for_platform


# =========================================================================
# Platform tests
# =========================================================================


class TestPlatform:
    """Tests for the Platform dataclass and PLATFORMS registry."""

    def test_platform_creation(self) -> None:
        """Verify a Platform can be created and all fields are accessible."""
        platform = Platform(
            platform_id="TEST",
            platform_name="Test Platform",
            platform_type="Floating Production",
            latitude=-22.5,
            longitude=-40.0,
            region="Santos Basin",
            timezone="America/Sao_Paulo",
        )
        assert platform.platform_id == "TEST"
        assert platform.platform_name == "Test Platform"
        assert platform.platform_type == "Floating Production"
        assert platform.latitude == -22.5
        assert platform.longitude == -40.0
        assert platform.region == "Santos Basin"
        assert platform.timezone == "America/Sao_Paulo"

    def test_all_platforms_defined(self) -> None:
        """Verify the PLATFORMS dict contains exactly 5 offshore installations."""
        assert len(PLATFORMS) == 5
        expected_ids = {"ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"}
        assert set(PLATFORMS.keys()) == expected_ids

    def test_platform_coordinates(self) -> None:
        """Verify all platforms have valid latitude (-90..90) and longitude (-180..180)."""
        for pid, platform in PLATFORMS.items():
            assert -90.0 <= platform.latitude <= 90.0, (
                f"Platform {pid} latitude out of range: {platform.latitude}"
            )
            assert -180.0 <= platform.longitude <= 180.0, (
                f"Platform {pid} longitude out of range: {platform.longitude}"
            )

    def test_platform_is_frozen(self, sample_platform: Platform) -> None:
        """Verify Platform is a frozen dataclass (immutable)."""
        with pytest.raises(AttributeError):
            sample_platform.platform_id = "MUTATED"  # type: ignore[misc]


# =========================================================================
# SensorType enum tests
# =========================================================================


class TestSensorType:
    """Tests for the SensorType StrEnum."""

    def test_sensor_type_enum(self) -> None:
        """Verify all 4 measurement types are defined in SensorType."""
        expected = {"TEMPERATURE", "PRESSURE", "VIBRATION", "FLOW_RATE"}
        actual = {member.value for member in SensorType}
        assert actual == expected

    def test_sensor_type_is_str(self) -> None:
        """Verify SensorType members behave as strings (StrEnum)."""
        assert isinstance(SensorType.TEMPERATURE, str)
        assert SensorType.TEMPERATURE == "TEMPERATURE"


# =========================================================================
# Sensor tests
# =========================================================================


class TestSensor:
    """Tests for the Sensor dataclass."""

    def test_sensor_creation(self, sample_sensor: Sensor) -> None:
        """Verify a Sensor can be created and all fields are accessible."""
        assert sample_sensor.sensor_id == "ALPHA-COMP-S01"
        assert sample_sensor.equipment_id == "ALPHA-COMP"
        assert sample_sensor.platform_id == "ALPHA"
        assert sample_sensor.sensor_type == SensorType.TEMPERATURE
        assert sample_sensor.unit == "degC"
        assert sample_sensor.min_range == 50.0
        assert sample_sensor.max_range == 250.0
        assert sample_sensor.subtype == "discharge_temp"

    def test_sensor_is_frozen(self, sample_sensor: Sensor) -> None:
        """Verify Sensor is a frozen dataclass (immutable)."""
        with pytest.raises(AttributeError):
            sample_sensor.value = 99.9  # type: ignore[misc]

    def test_generate_sensors_for_platform(self) -> None:
        """Verify sensor generation produces sensors for each equipment piece."""
        sensors = generate_sensors_for_platform("ALPHA")
        # 10 equipment x ~5 sensors each = ~50 sensors
        assert len(sensors) >= 40
        # All sensors should belong to ALPHA
        for s in sensors:
            assert s.platform_id == "ALPHA"

    def test_sensor_id_format(self) -> None:
        """Verify generated sensor IDs follow the {EQUIP_ID}-S{NN} pattern."""
        sensors = generate_sensors_for_platform("BRAVO")
        for s in sensors:
            assert "-S" in s.sensor_id, f"Unexpected sensor_id format: {s.sensor_id}"


# =========================================================================
# Equipment tests
# =========================================================================


class TestEquipment:
    """Tests for the Equipment dataclass and EQUIPMENT registry."""

    def test_equipment_creation(self) -> None:
        """Verify an Equipment can be created and all fields are accessible."""
        eq = Equipment(
            equipment_id="TEST-COMP",
            platform_id="TEST",
            equipment_name="Test Compressor",
            equipment_type="Compressor",
            manufacturer="Rolls-Royce",
            model="RB211-6762",
        )
        assert eq.equipment_id == "TEST-COMP"
        assert eq.platform_id == "TEST"
        assert eq.equipment_name == "Test Compressor"
        assert eq.equipment_type == "Compressor"
        assert eq.manufacturer == "Rolls-Royce"
        assert eq.model == "RB211-6762"

    def test_equipment_registry_populated(self) -> None:
        """Verify EQUIPMENT registry has entries for all 5 platforms (10 each)."""
        # 5 platforms x 10 equipment templates = 50 total
        assert len(EQUIPMENT) == 50

    def test_get_equipment_for_platform(self) -> None:
        """Verify get_equipment_for_platform returns exactly 10 items for ALPHA."""
        alpha_equipment = get_equipment_for_platform("ALPHA")
        assert len(alpha_equipment) == 10
        for eq in alpha_equipment:
            assert eq.platform_id == "ALPHA"

    def test_equipment_is_frozen(self) -> None:
        """Verify Equipment is a frozen dataclass (immutable)."""
        eq = EQUIPMENT["ALPHA-COMP"]
        with pytest.raises(AttributeError):
            eq.equipment_id = "MUTATED"  # type: ignore[misc]


# =========================================================================
# SensorReading tests
# =========================================================================


class TestSensorReading:
    """Tests for the SensorReading frozen dataclass."""

    def test_reading_creation(self, sample_reading: SensorReading) -> None:
        """Verify a SensorReading can be created and all fields are accessible."""
        assert sample_reading.reading_id == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert sample_reading.platform_id == "ALPHA"
        assert sample_reading.sensor_id == "ALPHA-COMP-S01"
        assert sample_reading.sensor_type == "TEMPERATURE"
        assert sample_reading.value == 85.3
        assert sample_reading.unit == "degC"
        assert sample_reading.timestamp == 1700000000000
        assert sample_reading.quality_flag == "GOOD"

    def test_reading_to_dict(
        self, sample_reading: SensorReading, sample_reading_dict: dict
    ) -> None:
        """Verify to_dict() output matches the expected dictionary representation."""
        result = sample_reading.to_dict()
        assert result == sample_reading_dict

    def test_reading_to_dict_keys(self, sample_reading: SensorReading) -> None:
        """Verify to_dict() contains all 8 expected keys."""
        expected_keys = {
            "reading_id",
            "platform_id",
            "sensor_id",
            "sensor_type",
            "value",
            "unit",
            "timestamp",
            "quality_flag",
        }
        assert set(sample_reading.to_dict().keys()) == expected_keys

    def test_reading_to_json(self, sample_reading: SensorReading) -> None:
        """Verify to_json() produces valid JSON that round-trips correctly."""
        json_str = sample_reading.to_json()
        parsed = json.loads(json_str)
        assert isinstance(parsed, dict)
        assert parsed["platform_id"] == "ALPHA"
        assert parsed["value"] == 85.3
        assert parsed["timestamp"] == 1700000000000

    def test_reading_to_json_is_valid_json(self, sample_reading: SensorReading) -> None:
        """Verify to_json() output is well-formed JSON (no exceptions on parse)."""
        json_str = sample_reading.to_json()
        # Should not raise
        json.loads(json_str)

    def test_reading_immutability(self, sample_reading: SensorReading) -> None:
        """Verify the frozen dataclass raises on attribute mutation."""
        with pytest.raises(AttributeError):
            sample_reading.value = 999.9  # type: ignore[misc]

        with pytest.raises(AttributeError):
            sample_reading.platform_id = "MUTATED"  # type: ignore[misc]

    def test_reading_unique_ids(self) -> None:
        """Verify default uuid factory generates unique IDs across instances."""
        readings = [SensorReading() for _ in range(100)]
        ids = [r.reading_id for r in readings]
        assert len(set(ids)) == 100, "Expected 100 unique reading IDs"

    def test_reading_default_values(self) -> None:
        """Verify SensorReading defaults are sensible when created empty."""
        reading = SensorReading()
        assert reading.platform_id == ""
        assert reading.sensor_id == ""
        assert reading.sensor_type == ""
        assert reading.value == 0.0
        assert reading.unit == ""
        assert reading.timestamp == 0
        assert reading.quality_flag == "GOOD"
        # reading_id should be a valid UUID string
        assert len(reading.reading_id) == 36  # UUID4 format: 8-4-4-4-12
