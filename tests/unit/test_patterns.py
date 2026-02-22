"""Unit tests for signal generation patterns.

Tests the signal generation functions that produce realistic sensor readings:
normal operating conditions, degradation drift, failure spikes, and seasonal
(diurnal) patterns. Also validates that SENSOR_CONFIGS covers all sensor
subtype combinations.

The ``src.patterns.signal`` module API:

    - ``SignalConfig(setpoint, noise_std, seasonal_amplitude, seasonal_period_hours)``
    - ``generate_normal(config, t_hours) -> float``
    - ``generate_degradation(config, t_hours, drift_rate) -> float``
    - ``generate_failure(config, spike_factor) -> float``
    - ``generate_seasonal(config, t_hours) -> float``
    - ``generate_reading(sensor, t_hours, pattern) -> float`` (dispatcher)
    - ``SENSOR_CONFIGS: dict[tuple[str, str], SignalConfig]``
"""

from __future__ import annotations

import math
import statistics

import numpy as np
import pytest

from src.patterns.signal import (
    SENSOR_CONFIGS,
    SignalConfig,
    generate_degradation,
    generate_failure,
    generate_normal,
    generate_reading,
    generate_seasonal,
)
from src.models.sensor import Sensor, SensorType


# =========================================================================
# SignalConfig tests
# =========================================================================


class TestSignalConfig:
    """Tests for the SignalConfig dataclass."""

    def test_signal_config(self) -> None:
        """Verify SignalConfig can be created and all fields are accessible."""
        config = SignalConfig(
            setpoint=100.0,
            noise_std=2.0,
            seasonal_amplitude=5.0,
            seasonal_period_hours=12.0,
        )
        assert config.setpoint == 100.0
        assert config.noise_std == 2.0
        assert config.seasonal_amplitude == 5.0
        assert config.seasonal_period_hours == 12.0

    def test_signal_config_defaults(self) -> None:
        """Verify default values for optional fields."""
        config = SignalConfig(setpoint=50.0, noise_std=1.0)
        assert config.seasonal_amplitude == 0.0
        assert config.seasonal_period_hours == 24.0

    def test_signal_config_is_frozen(self) -> None:
        """Verify SignalConfig is immutable."""
        config = SignalConfig(setpoint=100.0, noise_std=2.0)
        with pytest.raises(AttributeError):
            config.setpoint = 999.0  # type: ignore[misc]


# =========================================================================
# Normal signal generation tests
# =========================================================================


class TestGenerateNormal:
    """Tests for the ``generate_normal`` function."""

    def test_generate_normal_within_range(self) -> None:
        """Generate 1000 readings and verify mean is near setpoint, std near noise_std."""
        config = SignalConfig(setpoint=100.0, noise_std=2.0)
        values = [generate_normal(config, t_hours=0.0) for _ in range(1000)]

        assert len(values) == 1000

        mean_val = statistics.mean(values)
        std_val = statistics.stdev(values)

        # Mean should be within 3 standard errors of the setpoint
        se = config.noise_std / math.sqrt(1000)
        assert abs(mean_val - config.setpoint) < 5 * se, (
            f"Mean {mean_val:.4f} too far from setpoint {config.setpoint}"
        )

        # Standard deviation should be within 30% of the expected noise
        assert abs(std_val - config.noise_std) / config.noise_std < 0.30, (
            f"Std {std_val:.4f} too far from expected {config.noise_std}"
        )

    def test_generate_normal_reproducible(self) -> None:
        """With a fixed numpy RNG seed, verify deterministic output."""
        import src.patterns.signal as signal_mod

        config = SignalConfig(setpoint=50.0, noise_std=1.0)

        # Seed the module-level RNG, generate, then reseed and re-generate
        signal_mod._rng = np.random.default_rng(seed=42)
        values_a = [generate_normal(config, t_hours=0.0) for _ in range(50)]

        signal_mod._rng = np.random.default_rng(seed=42)
        values_b = [generate_normal(config, t_hours=0.0) for _ in range(50)]

        assert values_a == values_b, "Same RNG seed should produce identical sequences"

    def test_generate_normal_returns_float(self) -> None:
        """Verify generate_normal returns a single float."""
        config = SignalConfig(setpoint=50.0, noise_std=1.0)
        result = generate_normal(config, t_hours=1.0)
        assert isinstance(result, float)


# =========================================================================
# Degradation signal tests
# =========================================================================


class TestGenerateDegradation:
    """Tests for the ``generate_degradation`` function."""

    def test_generate_degradation_drift(self) -> None:
        """Verify values drift upward over time (positive drift_rate)."""
        config = SignalConfig(setpoint=100.0, noise_std=0.1)
        drift_rate = 2.0  # significant drift per hour

        # Generate samples at t=0 and t=100 hours
        early_values = [generate_degradation(config, t_hours=0.0, drift_rate=drift_rate)
                        for _ in range(50)]
        late_values = [generate_degradation(config, t_hours=100.0, drift_rate=drift_rate)
                       for _ in range(50)]

        early_mean = statistics.mean(early_values)
        late_mean = statistics.mean(late_values)

        assert late_mean > early_mean, (
            f"Expected upward drift: early_mean={early_mean:.2f}, late_mean={late_mean:.2f}"
        )

    def test_generate_degradation_includes_drift(self) -> None:
        """Verify the drift component is present in the output."""
        config = SignalConfig(setpoint=100.0, noise_std=0.0)
        drift_rate = 1.0

        # At t=10h with zero noise, result should be approximately setpoint + drift_rate * 10
        values = [generate_degradation(config, t_hours=10.0, drift_rate=drift_rate)
                  for _ in range(20)]
        mean_val = statistics.mean(values)

        expected = config.setpoint + drift_rate * 10.0
        assert abs(mean_val - expected) < 2.0, (
            f"Mean {mean_val:.2f} should be near {expected:.2f}"
        )

    def test_generate_degradation_returns_float(self) -> None:
        """Verify generate_degradation returns a single float."""
        config = SignalConfig(setpoint=100.0, noise_std=1.0)
        result = generate_degradation(config, t_hours=5.0)
        assert isinstance(result, float)


# =========================================================================
# Failure spike tests
# =========================================================================


class TestGenerateFailure:
    """Tests for the ``generate_failure`` function."""

    def test_generate_failure_spike(self) -> None:
        """Verify spike value is significantly away from the setpoint."""
        config = SignalConfig(setpoint=100.0, noise_std=2.0)
        spike_factor = 3.0

        spikes = [generate_failure(config, spike_factor=spike_factor) for _ in range(50)]

        # At least some spikes should be far from setpoint
        deviations = [abs(s - config.setpoint) for s in spikes]
        max_deviation = max(deviations)

        assert max_deviation > 2 * config.noise_std, (
            f"Max deviation {max_deviation:.2f} not significantly above "
            f"noise band ({2 * config.noise_std:.2f})"
        )

    def test_generate_failure_is_scalar(self) -> None:
        """Verify generate_failure returns a single float, not a list."""
        config = SignalConfig(setpoint=100.0, noise_std=1.0)
        result = generate_failure(config, spike_factor=2.0)
        assert isinstance(result, float)

    def test_generate_failure_magnitude(self) -> None:
        """Verify spike magnitude is approximately spike_factor * noise_std."""
        config = SignalConfig(setpoint=100.0, noise_std=5.0)
        spike_factor = 3.0

        spikes = [generate_failure(config, spike_factor=spike_factor) for _ in range(100)]
        deviations = [abs(s - config.setpoint) for s in spikes]
        mean_deviation = statistics.mean(deviations)

        expected = spike_factor * config.noise_std
        assert abs(mean_deviation - expected) < expected * 0.3, (
            f"Mean deviation {mean_deviation:.2f} too far from expected {expected:.2f}"
        )


# =========================================================================
# Seasonal signal tests
# =========================================================================


class TestGenerateSeasonal:
    """Tests for the ``generate_seasonal`` function."""

    def test_generate_seasonal_period(self) -> None:
        """Verify sinusoidal pattern over 24h (values oscillate around setpoint)."""
        config = SignalConfig(setpoint=100.0, noise_std=1.0, seasonal_amplitude=10.0)

        # Generate one full period: 24 samples (1 per hour)
        values = [generate_seasonal(config, t_hours=t) for t in range(24)]

        assert len(values) == 24

        # Values should oscillate: some above, some below setpoint
        above = [v for v in values if v > config.setpoint]
        below = [v for v in values if v < config.setpoint]
        assert len(above) > 0, "Expected some values above setpoint"
        assert len(below) > 0, "Expected some values below setpoint"

    def test_generate_seasonal_amplitude_bound(self) -> None:
        """Verify seasonal values stay within setpoint +/- amplitude."""
        config = SignalConfig(setpoint=50.0, noise_std=0.0, seasonal_amplitude=5.0)

        values = [generate_seasonal(config, t_hours=t * 0.5) for t in range(100)]
        for v in values:
            assert config.setpoint - config.seasonal_amplitude - 0.01 <= v, (
                f"Value {v:.4f} below lower bound"
            )
            assert v <= config.setpoint + config.seasonal_amplitude + 0.01, (
                f"Value {v:.4f} above upper bound"
            )

    def test_generate_seasonal_zero_amplitude(self) -> None:
        """Verify zero amplitude produces constant setpoint."""
        config = SignalConfig(setpoint=75.0, noise_std=0.0, seasonal_amplitude=0.0)
        values = [generate_seasonal(config, t_hours=t) for t in range(10)]
        for v in values:
            assert v == pytest.approx(config.setpoint), (
                f"With zero amplitude, expected {config.setpoint}, got {v}"
            )

    def test_generate_seasonal_returns_float(self) -> None:
        """Verify generate_seasonal returns a single float."""
        config = SignalConfig(setpoint=100.0, noise_std=0.0, seasonal_amplitude=5.0)
        result = generate_seasonal(config, t_hours=6.0)
        assert isinstance(result, float)


# =========================================================================
# SENSOR_CONFIGS registry tests
# =========================================================================


class TestSensorConfigs:
    """Tests for the SENSOR_CONFIGS dictionary."""

    def test_sensor_configs_defined(self) -> None:
        """Verify SENSOR_CONFIGS has entries (keyed by (sensor_type, subtype) tuples)."""
        assert len(SENSOR_CONFIGS) > 0, "SENSOR_CONFIGS should not be empty"

        # All keys should be (str, str) tuples
        for key in SENSOR_CONFIGS:
            assert isinstance(key, tuple), f"Key {key} is not a tuple"
            assert len(key) == 2, f"Key {key} should have 2 elements"
            assert isinstance(key[0], str) and isinstance(key[1], str)

    def test_sensor_configs_are_signal_config(self) -> None:
        """Verify all values in SENSOR_CONFIGS are SignalConfig instances."""
        for key, config in SENSOR_CONFIGS.items():
            assert isinstance(config, SignalConfig), (
                f"SENSOR_CONFIGS[{key}] is {type(config)}, expected SignalConfig"
            )

    def test_sensor_configs_positive_noise(self) -> None:
        """Verify all configs have positive noise_std values."""
        for key, config in SENSOR_CONFIGS.items():
            assert config.noise_std > 0, (
                f"SENSOR_CONFIGS[{key}].noise_std should be positive, got {config.noise_std}"
            )

    def test_sensor_configs_cover_all_sensor_types(self) -> None:
        """Verify every SensorType value appears at least once as a key prefix."""
        types_in_configs = {key[0] for key in SENSOR_CONFIGS}
        for sensor_type in SensorType:
            assert sensor_type.value in types_in_configs, (
                f"No SENSOR_CONFIGS entry for sensor type {sensor_type.value}"
            )

    def test_sensor_configs_positive_setpoints(self) -> None:
        """Verify all configs have positive setpoint values."""
        for key, config in SENSOR_CONFIGS.items():
            assert config.setpoint > 0, (
                f"SENSOR_CONFIGS[{key}].setpoint should be positive, got {config.setpoint}"
            )


# =========================================================================
# generate_reading dispatcher tests
# =========================================================================


class TestGenerateReading:
    """Tests for the ``generate_reading`` dispatcher function."""

    @pytest.fixture()
    def compressor_sensor(self) -> Sensor:
        """A compressor discharge temperature sensor for testing."""
        return Sensor(
            sensor_id="TEST-COMP-S01",
            equipment_id="TEST-COMP",
            platform_id="TEST",
            sensor_type=SensorType.TEMPERATURE,
            unit="degC",
            min_range=50.0,
            max_range=250.0,
            subtype="discharge_temp",
        )

    def test_generate_reading_normal(self, compressor_sensor: Sensor) -> None:
        """Verify generate_reading with 'normal' pattern returns a clamped value."""
        value = generate_reading(compressor_sensor, t_hours=5.0, pattern="normal")
        assert isinstance(value, float)
        assert compressor_sensor.min_range <= value <= compressor_sensor.max_range

    def test_generate_reading_degradation(self, compressor_sensor: Sensor) -> None:
        """Verify generate_reading with 'degradation' pattern returns a clamped value."""
        value = generate_reading(compressor_sensor, t_hours=5.0, pattern="degradation")
        assert isinstance(value, float)
        assert compressor_sensor.min_range <= value <= compressor_sensor.max_range

    def test_generate_reading_failure(self, compressor_sensor: Sensor) -> None:
        """Verify generate_reading with 'failure' pattern returns a clamped value."""
        value = generate_reading(compressor_sensor, t_hours=5.0, pattern="failure")
        assert isinstance(value, float)
        assert compressor_sensor.min_range <= value <= compressor_sensor.max_range

    def test_generate_reading_seasonal(self, compressor_sensor: Sensor) -> None:
        """Verify generate_reading with 'seasonal' pattern returns a clamped value."""
        value = generate_reading(compressor_sensor, t_hours=12.0, pattern="seasonal")
        assert isinstance(value, float)
        assert compressor_sensor.min_range <= value <= compressor_sensor.max_range

    def test_generate_reading_clamped_to_range(self) -> None:
        """Verify generate_reading clamps values to sensor's min/max range."""
        # Sensor with very narrow range to force clamping
        narrow_sensor = Sensor(
            sensor_id="TEST-NARROW-S01",
            equipment_id="TEST-NARROW",
            platform_id="TEST",
            sensor_type=SensorType.TEMPERATURE,
            unit="degC",
            min_range=99.0,
            max_range=101.0,
            subtype="discharge_temp",
        )
        for _ in range(50):
            value = generate_reading(narrow_sensor, t_hours=0.0, pattern="normal")
            assert narrow_sensor.min_range <= value <= narrow_sensor.max_range
