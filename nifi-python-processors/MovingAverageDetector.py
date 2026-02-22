"""NiFi Python Processor: Moving average deviation detector for sensor readings.

Maintains a rolling window of recent values per sensor_id and detects
statistical outliers using standard deviation analysis. Designed for
detecting gradual drift and sudden spikes in Oil & Gas sensor telemetry
where historical context is essential for accurate anomaly classification.
"""

import json
import logging
import time
from collections import deque

import numpy as np
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

logger = logging.getLogger("MovingAverageDetector")


class MovingAverageDetector(FlowFileTransform):
    """Detect anomalies using moving average and standard deviation analysis.

    Maintains an in-memory rolling window per sensor_id. For each incoming
    reading, the processor calculates the mean and standard deviation of
    the window, then determines if the current value deviates beyond a
    configurable number of standard deviations.

    State Management:
        The sensor windows are stored in-process memory. If the NiFi node
        restarts, the windows reset and require a warm-up period equal to
        the configured window size before anomaly detection becomes accurate.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Detects sensor anomalies using moving average and standard deviation "
            "analysis. Maintains a rolling window per sensor and flags readings "
            "that deviate beyond a configurable number of standard deviations."
        )
        tags = ["anomaly", "moving-average", "statistics", "sensor", "oilgas", "detection"]

    WINDOW_SIZE = PropertyDescriptor(
        name="Window Size",
        description=(
            "Number of recent readings to maintain per sensor for statistical "
            "analysis. Larger windows provide more stable baselines but respond "
            "slower to legitimate operational changes."
        ),
        default_value="50",
        required=True,
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
    )

    DEVIATION_THRESHOLD = PropertyDescriptor(
        name="Deviation Threshold",
        description=(
            "Number of standard deviations from the moving average that triggers "
            "an anomaly. Lower values increase sensitivity (more alerts); higher "
            "values reduce false positives. Typical range: 2.0-4.0."
        ),
        default_value="3.0",
        required=True,
        validators=[StandardValidators.POSITIVE_DOUBLE_VALIDATOR],
    )

    property_descriptors = [
        WINDOW_SIZE,
        DEVIATION_THRESHOLD,
    ]

    def __init__(self, **kwargs: object) -> None:
        super().__init__()
        self._sensor_windows: dict[str, deque[float]] = {}

    def getPropertyDescriptors(self) -> list[PropertyDescriptor]:
        return self.property_descriptors

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """Evaluate a sensor reading against its moving average baseline.

        Adds the current value to the sensor's rolling window, computes
        statistical metrics, and classifies the reading as anomalous if
        it exceeds the deviation threshold.

        Args:
            context: The processor context providing access to property values.
            flowfile: The incoming FlowFile containing a JSON sensor reading.

        Returns:
            FlowFileTransformResult with enriched JSON content, moving average
            attributes, and 'success' relationship.
        """
        window_size = int(context.getProperty(self.WINDOW_SIZE).getValue())
        deviation_threshold = float(context.getProperty(self.DEVIATION_THRESHOLD).getValue())

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

        window = self._get_or_create_window(sensor_id, window_size)
        window.append(value)

        severity, anomaly_type, deviation, mean, stddev, description = self._analyze(
            sensor_id=sensor_id,
            value=value,
            window=window,
            deviation_threshold=deviation_threshold,
            window_size=window_size,
        )

        detected_at = int(time.time() * 1000)

        reading["moving_average"] = {
            "severity": severity,
            "type": anomaly_type,
            "deviation": round(deviation, 4) if deviation is not None else None,
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "window_count": len(window),
            "window_size": window_size,
            "description": description,
            "detected_at": detected_at,
            "detector": "MovingAverageDetector",
        }

        attributes = {
            "anomaly.severity": severity,
            "anomaly.type": anomaly_type,
            "anomaly.detected_at": str(detected_at),
            "anomaly.detector": "MovingAverageDetector",
            "ma.deviation": str(round(deviation, 4)) if deviation is not None else "0.0",
            "ma.mean": str(round(mean, 4)),
            "ma.stddev": str(round(stddev, 4)),
            "ma.window_count": str(len(window)),
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

    def _get_or_create_window(self, sensor_id: str, window_size: int) -> deque[float]:
        """Retrieve or initialize the rolling window for a sensor.

        If the window exists but the configured size has changed, a new deque
        is created with the new maxlen, preserving as many recent values as
        possible.

        Args:
            sensor_id: The unique sensor identifier.
            window_size: Maximum number of values to retain.

        Returns:
            The deque for the given sensor_id.
        """
        existing = self._sensor_windows.get(sensor_id)

        if existing is not None and existing.maxlen == window_size:
            return existing

        if existing is not None and existing.maxlen != window_size:
            logger.info(
                "Window size changed for sensor %s: %d -> %d, rebuilding",
                sensor_id,
                existing.maxlen,
                window_size,
            )
            new_window: deque[float] = deque(existing, maxlen=window_size)
            self._sensor_windows[sensor_id] = new_window
            return new_window

        new_window = deque(maxlen=window_size)
        self._sensor_windows[sensor_id] = new_window
        return new_window

    @staticmethod
    def _analyze(
        sensor_id: str,
        value: float,
        window: deque[float],
        deviation_threshold: float,
        window_size: int,
    ) -> tuple[str, str, float | None, float, float, str]:
        """Perform statistical analysis on the sensor window.

        During the warm-up period (window not yet full), results are marked
        as NORMAL with a descriptive note. Once the window is populated,
        standard deviation analysis determines anomaly severity.

        Args:
            sensor_id: The sensor identifier for description text.
            value: The current sensor reading.
            window: The rolling window of recent values.
            deviation_threshold: Number of stddevs for anomaly classification.
            window_size: Configured window size for warm-up detection.

        Returns:
            Tuple of (severity, anomaly_type, deviation, mean, stddev, description).
        """
        window_array = np.array(window, dtype=np.float64)
        mean = float(np.mean(window_array))
        stddev = float(np.std(window_array, ddof=0))

        if len(window) < max(5, window_size // 2):
            return (
                "NORMAL",
                "none",
                None,
                mean,
                stddev,
                f"Sensor {sensor_id}: warming up ({len(window)}/{window_size} readings)",
            )

        if stddev < 1e-10:
            if abs(value - mean) < 1e-10:
                return (
                    "NORMAL",
                    "none",
                    0.0,
                    mean,
                    stddev,
                    f"Sensor {sensor_id}: constant value stream, no deviation",
                )
            return (
                "CRITICAL",
                "moving_average_deviation",
                float("inf"),
                mean,
                stddev,
                (
                    f"Sensor {sensor_id}: value {value} deviates from constant "
                    f"baseline {mean} (stddev ~0)"
                ),
            )

        deviation = abs(value - mean) / stddev

        if deviation > deviation_threshold * 1.5:
            return (
                "CRITICAL",
                "moving_average_deviation",
                deviation,
                mean,
                stddev,
                (
                    f"Sensor {sensor_id}: value {value} is {deviation:.2f} stddevs "
                    f"from mean {mean:.2f} (critical > {deviation_threshold * 1.5:.1f})"
                ),
            )

        if deviation > deviation_threshold:
            return (
                "WARNING",
                "moving_average_deviation",
                deviation,
                mean,
                stddev,
                (
                    f"Sensor {sensor_id}: value {value} is {deviation:.2f} stddevs "
                    f"from mean {mean:.2f} (warning > {deviation_threshold:.1f})"
                ),
            )

        return (
            "NORMAL",
            "none",
            deviation,
            mean,
            stddev,
            f"Sensor {sensor_id}: value {value} within {deviation:.2f} stddevs of mean {mean:.2f}",
        )
