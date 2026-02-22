"""Domain models for the Oil & Gas data generator."""

from src.models.equipment import EQUIPMENT, Equipment, get_equipment_for_platform
from src.models.platform import PLATFORMS, Platform
from src.models.reading import SensorReading
from src.models.sensor import Sensor, SensorType, generate_sensors_for_platform

__all__ = [
    "EQUIPMENT",
    "Equipment",
    "PLATFORMS",
    "Platform",
    "Sensor",
    "SensorReading",
    "SensorType",
    "generate_sensors_for_platform",
    "get_equipment_for_platform",
]
