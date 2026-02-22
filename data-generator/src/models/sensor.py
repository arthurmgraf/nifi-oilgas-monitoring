"""Sensor definitions and automatic sensor generation for each platform.

Each platform gets ~50 sensors distributed across its equipment, covering
temperature, pressure, vibration, and flow-rate measurement types.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from src.models.equipment import get_equipment_for_platform


class SensorType(StrEnum):
    """Physical measurement types collected by platform sensors."""

    TEMPERATURE = "TEMPERATURE"
    PRESSURE = "PRESSURE"
    VIBRATION = "VIBRATION"
    FLOW_RATE = "FLOW_RATE"


# Mapping: (equipment_type) -> list of (sensor_type, unit, min_range, max_range, subtype_label)
_SENSOR_SPECS: dict[str, list[tuple[SensorType, str, float, float, str]]] = {
    "Compressor": [
        (SensorType.TEMPERATURE, "degC", 50.0, 250.0, "discharge_temp"),
        (SensorType.PRESSURE, "bar", 10.0, 80.0, "discharge_pressure"),
        (SensorType.VIBRATION, "mm/s", 0.0, 25.0, "bearing_vibration"),
        (SensorType.TEMPERATURE, "degC", 40.0, 120.0, "oil_temp"),
        (SensorType.PRESSURE, "bar", 2.0, 10.0, "oil_pressure"),
    ],
    "Separator": [
        (SensorType.PRESSURE, "bar", 5.0, 50.0, "vessel_pressure"),
        (SensorType.TEMPERATURE, "degC", 30.0, 150.0, "process_temp"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 500.0, "oil_outlet_flow"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 300.0, "water_outlet_flow"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 1000.0, "gas_outlet_flow"),
    ],
    "Pump": [
        (SensorType.PRESSURE, "bar", 50.0, 350.0, "discharge_pressure"),
        (SensorType.TEMPERATURE, "degC", 30.0, 100.0, "bearing_temp"),
        (SensorType.VIBRATION, "mm/s", 0.0, 20.0, "motor_vibration"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 200.0, "injection_flow"),
        (SensorType.PRESSURE, "bar", 1.0, 10.0, "suction_pressure"),
    ],
    "Turbine": [
        (SensorType.TEMPERATURE, "degC", 200.0, 600.0, "exhaust_temp"),
        (SensorType.VIBRATION, "mm/s", 0.0, 15.0, "shaft_vibration"),
        (SensorType.PRESSURE, "bar", 8.0, 30.0, "inlet_pressure"),
        (SensorType.TEMPERATURE, "degC", 50.0, 200.0, "lube_oil_temp"),
        (SensorType.FLOW_RATE, "m3/h", 100.0, 5000.0, "fuel_gas_flow"),
    ],
    "Heat Exchanger": [
        (SensorType.TEMPERATURE, "degC", 30.0, 200.0, "inlet_temp"),
        (SensorType.TEMPERATURE, "degC", 20.0, 150.0, "outlet_temp"),
        (SensorType.PRESSURE, "bar", 5.0, 40.0, "shell_pressure"),
        (SensorType.PRESSURE, "bar", 5.0, 40.0, "tube_pressure"),
        (SensorType.FLOW_RATE, "m3/h", 10.0, 300.0, "process_flow"),
    ],
    "Wellhead": [
        (SensorType.PRESSURE, "bar", 100.0, 700.0, "tubing_pressure"),
        (SensorType.PRESSURE, "bar", 50.0, 500.0, "casing_pressure"),
        (SensorType.TEMPERATURE, "degC", 60.0, 180.0, "wellhead_temp"),
        (SensorType.FLOW_RATE, "m3/h", 5.0, 200.0, "production_flow"),
        (SensorType.VIBRATION, "mm/s", 0.0, 10.0, "choke_vibration"),
    ],
    "Riser": [
        (SensorType.PRESSURE, "bar", 20.0, 300.0, "riser_pressure"),
        (SensorType.TEMPERATURE, "degC", 4.0, 100.0, "riser_temp"),
        (SensorType.VIBRATION, "mm/s", 0.0, 30.0, "viv_sensor"),
        (SensorType.FLOW_RATE, "m3/h", 10.0, 500.0, "throughput_flow"),
        (SensorType.PRESSURE, "bar", 1.0, 20.0, "annulus_pressure"),
    ],
    "BOP": [
        (SensorType.PRESSURE, "bar", 200.0, 1034.0, "stack_pressure"),
        (SensorType.PRESSURE, "bar", 150.0, 700.0, "accumulator_pressure"),
        (SensorType.TEMPERATURE, "degC", 10.0, 80.0, "hydraulic_temp"),
        (SensorType.VIBRATION, "mm/s", 0.0, 5.0, "stack_vibration"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 50.0, "hydraulic_flow"),
    ],
    "Flare": [
        (SensorType.TEMPERATURE, "degC", 300.0, 1200.0, "flame_temp"),
        (SensorType.FLOW_RATE, "m3/h", 0.0, 10000.0, "gas_flow"),
        (SensorType.PRESSURE, "bar", 0.5, 5.0, "header_pressure"),
        (SensorType.TEMPERATURE, "degC", 50.0, 300.0, "tip_temp"),
        (SensorType.VIBRATION, "mm/s", 0.0, 20.0, "structural_vibration"),
    ],
}


@dataclass(frozen=True)
class Sensor:
    """A single sensor installed on a piece of equipment."""

    sensor_id: str
    equipment_id: str
    platform_id: str
    sensor_type: SensorType
    unit: str
    min_range: float
    max_range: float
    subtype: str = ""


def generate_sensors_for_platform(platform_id: str) -> list[Sensor]:
    """Create the full sensor inventory for one platform.

    Each piece of equipment gets 5 sensors based on its type, yielding
    approximately 50 sensors per platform (10 equipment x 5 sensors each).
    """
    sensors: list[Sensor] = []
    equipment_list = get_equipment_for_platform(platform_id)

    for equipment in equipment_list:
        specs = _SENSOR_SPECS.get(equipment.equipment_type, [])
        for idx, (s_type, unit, min_r, max_r, subtype) in enumerate(specs, start=1):
            sensor_id = f"{equipment.equipment_id}-S{idx:02d}"
            sensors.append(
                Sensor(
                    sensor_id=sensor_id,
                    equipment_id=equipment.equipment_id,
                    platform_id=platform_id,
                    sensor_type=s_type,
                    unit=unit,
                    min_range=min_r,
                    max_range=max_r,
                    subtype=subtype,
                )
            )

    return sensors
