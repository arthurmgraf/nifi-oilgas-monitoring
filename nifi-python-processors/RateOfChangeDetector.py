"""NiFi Python Processor: Rate of change spike detection for sensor readings.

Tracks consecutive readings per sensor_id and detects sudden value changes
(spikes) by computing the rate of change over time. Critical for Oil & Gas
monitoring where rapid pressure drops, temperature surges, or vibration
spikes indicate imminent equipment failure or safety hazards.
"""

import json
import logging
import time
from dataclasses import dataclass

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

logger = logging.getLogger("RateOfChangeDetector")


@dataclass(frozen=True)
class SensorState:
    """Immutable snapshot of the last known reading for a sensor."""

    value: float
    timestamp_ms: int


class RateOfChangeDetector(FlowFileTransform):
    """Detect anomalies by tracking the rate of change between consecutive readings.

    Maintains the last known value and timestamp per sensor_id. For each
    incoming reading, computes:

        rate = abs(current_value - last_value) / time_delta_seconds

    If the rate exceeds the configured maximum, the reading is flagged as
    a spike anomaly. A configurable time window filters out stale comparisons
    where the gap between readings is too large for meaningful rate analysis.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Detects sudden spikes in sensor values by computing the rate of "
            "change between consecutive readings. Flags readings where the rate "
            "exceeds a configurable threshold."
        )
        tags = ["anomaly", "rate-of-change", "spike", "sensor", "oilgas", "detection"]

    MAX_RATE_OF_CHANGE = PropertyDescriptor(
        name="Max Rate of Change",
        description=(
            "Maximum allowed rate of change (units per second). Readings with a "
            "rate exceeding this value are classified as spikes. The appropriate "
            "value depends on the sensor type and expected operational dynamics."
        ),
        default_value="10.0",
        required=True,
        validators=[StandardValidators.POSITIVE_DOUBLE_VALIDATOR],
    )

    TIME_WINDOW_SECONDS = PropertyDescriptor(
        name="Time Window Seconds",
        description=(
            "Maximum time gap (seconds) between consecutive readings for rate "
            "calculation. If the gap exceeds this window, the comparison is "
            "skipped and the reading is treated as a new baseline."
        ),
        default_value="60",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
    )

    property_descriptors = [
        MAX_RATE_OF_CHANGE,
        TIME_WINDOW_SECONDS,
    ]

    def __init__(self, **kwargs: object) -> None:
        super().__init__()
        self._sensor_states: dict[str, SensorState] = {}

    def getPropertyDescriptors(self) -> list[PropertyDescriptor]:
        return self.property_descriptors

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """Evaluate the rate of change for a sensor reading.

        Compares the current value against the last known value for the same
        sensor_id. If no previous state exists or the time gap exceeds the
        configured window, the reading becomes the new baseline.

        Args:
            context: The processor context providing access to property values.
            flowfile: The incoming FlowFile containing a JSON sensor reading.

        Returns:
            FlowFileTransformResult with enriched JSON content, rate-of-change
            attributes, and 'success' relationship.
        """
        max_rate = float(context.getProperty(self.MAX_RATE_OF_CHANGE).getValue())
        time_window = int(context.getProperty(self.TIME_WINDOW_SECONDS).getValue())

        contents = flowfile.getContentsAsBytes().decode("utf-8")

        try:
            reading = json.loads(contents)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to parse FlowFile JSON: %s", exc)
            return FlowFileTransformResult(
                relationship="failure",
                contents=contents,
                attributes={
                    "anomaly.error": f"JSON parse error: {exc}",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        sensor_id = reading.get("sensor_id")
        if not sensor_id:
            logger.warning("Sensor reading missing 'sensor_id': %s", reading.get("reading_id"))
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps(reading),
                attributes={
                    "anomaly.error": "Missing 'sensor_id' field",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        value = reading.get("value")
        if value is None:
            logger.warning("Sensor reading missing 'value': sensor_id=%s", sensor_id)
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps(reading),
                attributes={
                    "anomaly.error": "Missing 'value' field",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        try:
            value = float(value)
        except (TypeError, ValueError) as exc:
            logger.warning("Non-numeric value '%s' for sensor %s: %s", value, sensor_id, exc)
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps(reading),
                attributes={
                    "anomaly.error": f"Non-numeric value: {value}",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        current_timestamp_ms = self._extract_timestamp(reading)

        severity, is_spike, rate, description = self._analyze(
            sensor_id=sensor_id,
            value=value,
            timestamp_ms=current_timestamp_ms,
            max_rate=max_rate,
            time_window_seconds=time_window,
        )

        self._sensor_states[sensor_id] = SensorState(
            value=value,
            timestamp_ms=current_timestamp_ms,
        )

        detected_at = int(time.time() * 1000)

        reading["rate_of_change"] = {
            "severity": severity,
            "is_spike": is_spike,
            "rate": round(rate, 6) if rate is not None else None,
            "max_rate_threshold": max_rate,
            "description": description,
            "detected_at": detected_at,
            "detector": "RateOfChangeDetector",
        }

        attributes = {
            "anomaly.severity": severity,
            "anomaly.type": "rate_of_change_spike" if is_spike else "none",
            "anomaly.detected_at": str(detected_at),
            "anomaly.detector": "RateOfChangeDetector",
            "roc.rate": str(round(rate, 6)) if rate is not None else "0.0",
            "roc.is_spike": str(is_spike).lower(),
        }

        if reading.get("sensor_id"):
            attributes["sensor.id"] = reading["sensor_id"]
        if reading.get("platform_id"):
            attributes["platform.id"] = reading["platform_id"]

        return FlowFileTransformResult(
            relationship="success",
            contents=json.dumps(reading),
            attributes=attributes,
        )

    def _analyze(
        self,
        sensor_id: str,
        value: float,
        timestamp_ms: int,
        max_rate: float,
        time_window_seconds: int,
    ) -> tuple[str, bool, float | None, str]:
        """Compute rate of change and determine if a spike occurred.

        Args:
            sensor_id: The sensor identifier.
            value: Current sensor reading value.
            timestamp_ms: Current reading timestamp in epoch milliseconds.
            max_rate: Maximum allowed rate (units/second).
            time_window_seconds: Maximum gap for valid comparison.

        Returns:
            Tuple of (severity, is_spike, rate, description).
        """
        previous = self._sensor_states.get(sensor_id)

        if previous is None:
            return (
                "NORMAL",
                False,
                None,
                f"Sensor {sensor_id}: first reading, establishing baseline",
            )

        time_delta_ms = timestamp_ms - previous.timestamp_ms
        time_delta_seconds = time_delta_ms / 1000.0

        if time_delta_seconds <= 0:
            logger.warning(
                "Non-positive time delta for sensor %s: %d ms (current=%d, previous=%d)",
                sensor_id,
                time_delta_ms,
                timestamp_ms,
                previous.timestamp_ms,
            )
            return (
                "NORMAL",
                False,
                None,
                (
                    f"Sensor {sensor_id}: non-positive time delta "
                    f"({time_delta_seconds:.3f}s), skipping rate calculation"
                ),
            )

        if time_delta_seconds > time_window_seconds:
            logger.info(
                "Time gap %.1fs exceeds window %ds for sensor %s, resetting baseline",
                time_delta_seconds,
                time_window_seconds,
                sensor_id,
            )
            return (
                "NORMAL",
                False,
                None,
                (
                    f"Sensor {sensor_id}: time gap {time_delta_seconds:.1f}s exceeds "
                    f"window {time_window_seconds}s, new baseline"
                ),
            )

        value_delta = abs(value - previous.value)
        rate = value_delta / time_delta_seconds

        if rate > max_rate * 2.0:
            return (
                "CRITICAL",
                True,
                rate,
                (
                    f"Sensor {sensor_id}: critical spike detected, rate {rate:.4f} "
                    f"units/s (>{max_rate * 2.0:.1f}, 2x threshold) over "
                    f"{time_delta_seconds:.1f}s"
                ),
            )

        if rate > max_rate:
            return (
                "WARNING",
                True,
                rate,
                (
                    f"Sensor {sensor_id}: spike detected, rate {rate:.4f} "
                    f"units/s (>{max_rate:.1f}) over {time_delta_seconds:.1f}s"
                ),
            )

        return (
            "NORMAL",
            False,
            rate,
            (
                f"Sensor {sensor_id}: rate {rate:.4f} units/s within "
                f"threshold {max_rate:.1f} over {time_delta_seconds:.1f}s"
            ),
        )

    @staticmethod
    def _extract_timestamp(reading: dict) -> int:
        """Extract timestamp from reading, falling back to current time.

        Supports the Avro schema's timestamp-millis (epoch milliseconds)
        format as well as ISO 8601 strings.

        Args:
            reading: The parsed sensor reading dictionary.

        Returns:
            Timestamp as epoch milliseconds.
        """
        ts = reading.get("timestamp")

        if ts is None:
            return int(time.time() * 1000)

        if isinstance(ts, (int, float)):
            ts_val = int(ts)
            if ts_val < 1e12:
                return ts_val * 1000
            return ts_val

        if isinstance(ts, str):
            try:
                return int(float(ts))
            except (TypeError, ValueError):
                logger.warning("Unable to parse timestamp string '%s', using current time", ts)
                return int(time.time() * 1000)

        return int(time.time() * 1000)
