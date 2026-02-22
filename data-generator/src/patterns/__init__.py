"""Signal pattern generation for simulated sensor data."""

from src.patterns.signal import (
    SENSOR_CONFIGS,
    SignalConfig,
    generate_degradation,
    generate_failure,
    generate_normal,
    generate_reading,
    generate_seasonal,
)

__all__ = [
    "SENSOR_CONFIGS",
    "SignalConfig",
    "generate_degradation",
    "generate_failure",
    "generate_normal",
    "generate_reading",
    "generate_seasonal",
]
