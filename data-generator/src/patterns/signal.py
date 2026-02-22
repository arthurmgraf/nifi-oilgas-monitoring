"""Signal generation functions for realistic Oil & Gas sensor data.

Uses numpy for efficient generation of Gaussian noise, seasonal
patterns, degradation drifts, and failure spikes.  Every sensor type
has a pre-configured ``SignalConfig`` that governs its baseline
behaviour so that generated values are physically plausible.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from src.models.sensor import Sensor


@dataclass(frozen=True)
class SignalConfig:
    """Parameters that govern a sensor's normal operating signal."""

    setpoint: float
    noise_std: float
    seasonal_amplitude: float = 0.0
    seasonal_period_hours: float = 24.0


# ── Realistic defaults per (sensor_type, subtype) ──────────────────────
# Keys are (SensorType.value, subtype_label).
SENSOR_CONFIGS: dict[tuple[str, str], SignalConfig] = {
    # Compressor
    ("TEMPERATURE", "discharge_temp"): SignalConfig(
        setpoint=160.0, noise_std=3.0, seasonal_amplitude=5.0
    ),
    ("PRESSURE", "discharge_pressure"): SignalConfig(
        setpoint=45.0, noise_std=1.5, seasonal_amplitude=2.0
    ),
    ("VIBRATION", "bearing_vibration"): SignalConfig(setpoint=4.5, noise_std=0.8),
    ("TEMPERATURE", "oil_temp"): SignalConfig(setpoint=75.0, noise_std=2.0, seasonal_amplitude=3.0),
    ("PRESSURE", "oil_pressure"): SignalConfig(setpoint=5.5, noise_std=0.3),
    # Separator
    ("PRESSURE", "vessel_pressure"): SignalConfig(
        setpoint=28.0, noise_std=1.0, seasonal_amplitude=1.5
    ),
    ("TEMPERATURE", "process_temp"): SignalConfig(
        setpoint=85.0, noise_std=2.5, seasonal_amplitude=4.0
    ),
    ("FLOW_RATE", "oil_outlet_flow"): SignalConfig(
        setpoint=250.0, noise_std=15.0, seasonal_amplitude=20.0, seasonal_period_hours=12.0
    ),
    ("FLOW_RATE", "water_outlet_flow"): SignalConfig(
        setpoint=120.0, noise_std=10.0, seasonal_amplitude=15.0, seasonal_period_hours=12.0
    ),
    ("FLOW_RATE", "gas_outlet_flow"): SignalConfig(
        setpoint=500.0, noise_std=30.0, seasonal_amplitude=40.0, seasonal_period_hours=12.0
    ),
    # Pump
    ("PRESSURE", "pump_discharge"): SignalConfig(setpoint=200.0, noise_std=5.0),
    ("TEMPERATURE", "bearing_temp"): SignalConfig(
        setpoint=55.0, noise_std=1.5, seasonal_amplitude=3.0
    ),
    ("VIBRATION", "motor_vibration"): SignalConfig(setpoint=3.5, noise_std=0.6),
    ("FLOW_RATE", "injection_flow"): SignalConfig(
        setpoint=100.0, noise_std=8.0, seasonal_amplitude=10.0, seasonal_period_hours=8.0
    ),
    ("PRESSURE", "suction_pressure"): SignalConfig(setpoint=4.0, noise_std=0.5),
    # Turbine
    ("TEMPERATURE", "exhaust_temp"): SignalConfig(
        setpoint=420.0, noise_std=10.0, seasonal_amplitude=15.0
    ),
    ("VIBRATION", "shaft_vibration"): SignalConfig(setpoint=3.0, noise_std=0.5),
    ("PRESSURE", "inlet_pressure"): SignalConfig(setpoint=18.0, noise_std=1.0),
    ("TEMPERATURE", "lube_oil_temp"): SignalConfig(
        setpoint=90.0, noise_std=2.0, seasonal_amplitude=5.0
    ),
    ("FLOW_RATE", "fuel_gas_flow"): SignalConfig(
        setpoint=2500.0, noise_std=100.0, seasonal_amplitude=200.0, seasonal_period_hours=6.0
    ),
    # Heat Exchanger
    ("TEMPERATURE", "inlet_temp"): SignalConfig(
        setpoint=120.0, noise_std=3.0, seasonal_amplitude=8.0
    ),
    ("TEMPERATURE", "outlet_temp"): SignalConfig(
        setpoint=65.0, noise_std=2.0, seasonal_amplitude=5.0
    ),
    ("PRESSURE", "shell_pressure"): SignalConfig(setpoint=22.0, noise_std=1.0),
    ("PRESSURE", "tube_pressure"): SignalConfig(setpoint=20.0, noise_std=1.0),
    ("FLOW_RATE", "process_flow"): SignalConfig(
        setpoint=150.0, noise_std=10.0, seasonal_amplitude=12.0
    ),
    # Wellhead
    ("PRESSURE", "tubing_pressure"): SignalConfig(
        setpoint=350.0, noise_std=8.0, seasonal_amplitude=10.0
    ),
    ("PRESSURE", "casing_pressure"): SignalConfig(setpoint=220.0, noise_std=5.0),
    ("TEMPERATURE", "wellhead_temp"): SignalConfig(
        setpoint=110.0, noise_std=3.0, seasonal_amplitude=6.0
    ),
    ("FLOW_RATE", "production_flow"): SignalConfig(
        setpoint=80.0, noise_std=5.0, seasonal_amplitude=8.0, seasonal_period_hours=12.0
    ),
    ("VIBRATION", "choke_vibration"): SignalConfig(setpoint=2.0, noise_std=0.4),
    # Riser
    ("PRESSURE", "riser_pressure"): SignalConfig(setpoint=150.0, noise_std=4.0),
    ("TEMPERATURE", "riser_temp"): SignalConfig(
        setpoint=45.0, noise_std=2.0, seasonal_amplitude=5.0
    ),
    ("VIBRATION", "viv_sensor"): SignalConfig(
        setpoint=5.0, noise_std=1.5, seasonal_amplitude=3.0, seasonal_period_hours=6.0
    ),
    ("FLOW_RATE", "throughput_flow"): SignalConfig(
        setpoint=250.0, noise_std=15.0, seasonal_amplitude=20.0
    ),
    ("PRESSURE", "annulus_pressure"): SignalConfig(setpoint=8.0, noise_std=0.5),
    # BOP
    ("PRESSURE", "stack_pressure"): SignalConfig(setpoint=690.0, noise_std=10.0),
    ("PRESSURE", "accumulator_pressure"): SignalConfig(setpoint=350.0, noise_std=5.0),
    ("TEMPERATURE", "hydraulic_temp"): SignalConfig(
        setpoint=45.0, noise_std=2.0, seasonal_amplitude=4.0
    ),
    ("VIBRATION", "stack_vibration"): SignalConfig(setpoint=1.0, noise_std=0.2),
    ("FLOW_RATE", "hydraulic_flow"): SignalConfig(setpoint=20.0, noise_std=2.0),
    # Flare
    ("TEMPERATURE", "flame_temp"): SignalConfig(
        setpoint=800.0, noise_std=50.0, seasonal_amplitude=60.0
    ),
    ("FLOW_RATE", "gas_flow"): SignalConfig(
        setpoint=4000.0, noise_std=300.0, seasonal_amplitude=500.0, seasonal_period_hours=8.0
    ),
    ("PRESSURE", "header_pressure"): SignalConfig(setpoint=2.5, noise_std=0.3),
    ("TEMPERATURE", "tip_temp"): SignalConfig(
        setpoint=180.0, noise_std=10.0, seasonal_amplitude=15.0
    ),
    ("VIBRATION", "structural_vibration"): SignalConfig(
        setpoint=6.0, noise_std=1.5, seasonal_amplitude=2.0
    ),
}

# Fallback config used when no specific mapping is found.
_DEFAULT_CONFIG = SignalConfig(setpoint=50.0, noise_std=5.0)

_rng = np.random.default_rng()


def _get_config(sensor: Sensor) -> SignalConfig:
    """Look up the signal config for a sensor, falling back to a default."""
    key = (sensor.sensor_type.value, sensor.subtype)
    return SENSOR_CONFIGS.get(key, _DEFAULT_CONFIG)


# ── Signal generators ──────────────────────────────────────────────────


def generate_normal(config: SignalConfig, t_hours: float) -> float:
    """Gaussian noise around the setpoint with optional seasonal component."""
    base = config.setpoint + float(_rng.normal(0, config.noise_std))
    seasonal = _seasonal_component(config, t_hours)
    return base + seasonal


def generate_degradation(
    config: SignalConfig,
    t_hours: float,
    drift_rate: float = 0.5,
) -> float:
    """Linear drift from the setpoint simulating equipment degradation."""
    drift = drift_rate * t_hours
    return generate_normal(config, t_hours) + drift


def generate_failure(config: SignalConfig, spike_factor: float = 3.0) -> float:
    """Sudden spike well outside normal range -- simulates abrupt failure."""
    direction = 1.0 if _rng.random() > 0.5 else -1.0
    spike = direction * spike_factor * config.noise_std
    return config.setpoint + spike


def generate_seasonal(config: SignalConfig, t_hours: float) -> float:
    """Pure sinusoidal variation without noise overlay."""
    return config.setpoint + _seasonal_component(config, t_hours)


def _seasonal_component(config: SignalConfig, t_hours: float) -> float:
    """Compute the sinusoidal seasonal offset."""
    if config.seasonal_amplitude == 0.0:
        return 0.0
    return config.seasonal_amplitude * math.sin(
        2.0 * math.pi * t_hours / config.seasonal_period_hours
    )


# ── Dispatcher ─────────────────────────────────────────────────────────


def generate_reading(
    sensor: Sensor,
    t_hours: float,
    pattern: str = "normal",
) -> float:
    """Generate a single sensor value using the requested pattern.

    Supported patterns: ``normal``, ``degradation``, ``failure``, ``seasonal``.
    The value is clamped to the sensor's physical min/max range.
    """
    config = _get_config(sensor)

    match pattern:
        case "normal":
            value = generate_normal(config, t_hours)
        case "degradation":
            value = generate_degradation(config, t_hours)
        case "failure":
            value = generate_failure(config)
        case "seasonal":
            value = generate_seasonal(config, t_hours)
        case _:
            value = generate_normal(config, t_hours)

    # Clamp to physical sensor range
    return max(sensor.min_range, min(sensor.max_range, value))
