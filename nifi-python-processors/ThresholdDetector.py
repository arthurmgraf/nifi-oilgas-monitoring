"""NiFi Python Processor: Threshold-based anomaly detection for sensor readings.

Compares sensor values against configurable high/low thresholds and classifies
readings as NORMAL, WARNING, or CRITICAL. Designed for Oil & Gas upstream
monitoring where early detection of out-of-range values prevents equipment
failure and safety incidents.
"""

import json
import logging
import time

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

logger = logging.getLogger("ThresholdDetector")


class ThresholdDetector(FlowFileTransform):
    """Detect anomalies by comparing sensor values against configurable thresholds.

    Evaluates each sensor reading against four threshold boundaries:
    - Critical High: value >= threshold triggers CRITICAL severity
    - Warning High:  value >= threshold triggers WARNING severity
    - Warning Low:   value <= threshold triggers WARNING severity
    - Critical Low:  value <= threshold triggers CRITICAL severity

    Outputs enriched JSON with anomaly metadata and sets FlowFile attributes
    for downstream routing.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Detects sensor anomalies by comparing values against configurable "
            "high/low thresholds. Classifies readings as NORMAL, WARNING, or CRITICAL."
        )
        tags = ["anomaly", "threshold", "sensor", "oilgas", "monitoring", "detection"]

    CRITICAL_HIGH_THRESHOLD = PropertyDescriptor(
        name="Critical High Threshold",
        description=(
            "Value at or above which a CRITICAL anomaly is triggered. "
            "Should be the absolute upper safety limit for the monitored metric."
        ),
        default_value="120.0",
        required=True,
        validators=[StandardValidators.POSITIVE_DOUBLE_VALIDATOR],
    )

    WARNING_HIGH_THRESHOLD = PropertyDescriptor(
        name="Warning High Threshold",
        description=(
            "Value at or above which a WARNING anomaly is triggered. "
            "Should be below Critical High to allow early intervention."
        ),
        default_value="100.0",
        required=True,
        validators=[StandardValidators.POSITIVE_DOUBLE_VALIDATOR],
    )

    CRITICAL_LOW_THRESHOLD = PropertyDescriptor(
        name="Critical Low Threshold",
        description=(
            "Value at or below which a CRITICAL anomaly is triggered. "
            "Represents the absolute lower safety limit."
        ),
        default_value="5.0",
        required=True,
        validators=[StandardValidators.NON_NEGATIVE_DOUBLE_VALIDATOR],
    )

    WARNING_LOW_THRESHOLD = PropertyDescriptor(
        name="Warning Low Threshold",
        description=(
            "Value at or below which a WARNING anomaly is triggered. "
            "Should be above Critical Low to provide early warning."
        ),
        default_value="10.0",
        required=True,
        validators=[StandardValidators.NON_NEGATIVE_DOUBLE_VALIDATOR],
    )

    property_descriptors = [
        CRITICAL_HIGH_THRESHOLD,
        WARNING_HIGH_THRESHOLD,
        CRITICAL_LOW_THRESHOLD,
        WARNING_LOW_THRESHOLD,
    ]

    def __init__(self, **kwargs: object) -> None:
        super().__init__()

    def getPropertyDescriptors(self) -> list[PropertyDescriptor]:
        return self.property_descriptors

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """Evaluate a sensor reading against configured thresholds.

        Reads JSON input containing at minimum a 'value' field, compares it
        against the four threshold boundaries, and returns enriched JSON with
        anomaly classification.

        Args:
            context: The processor context providing access to property values.
            flowfile: The incoming FlowFile containing a JSON sensor reading.

        Returns:
            FlowFileTransformResult with enriched JSON content, anomaly
            attributes, and 'success' relationship.
        """
        critical_high = float(context.getProperty(self.CRITICAL_HIGH_THRESHOLD).getValue())
        warning_high = float(context.getProperty(self.WARNING_HIGH_THRESHOLD).getValue())
        critical_low = float(context.getProperty(self.CRITICAL_LOW_THRESHOLD).getValue())
        warning_low = float(context.getProperty(self.WARNING_LOW_THRESHOLD).getValue())

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

        value = reading.get("value")
        if value is None:
            logger.warning("Sensor reading missing 'value' field: %s", reading.get("reading_id"))
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps(reading),
                attributes={
                    "anomaly.error": "Missing 'value' field in sensor reading",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        try:
            value = float(value)
        except (TypeError, ValueError) as exc:
            logger.warning("Non-numeric value '%s': %s", value, exc)
            return FlowFileTransformResult(
                relationship="failure",
                contents=json.dumps(reading),
                attributes={
                    "anomaly.error": f"Non-numeric value: {value}",
                    "anomaly.severity": "UNKNOWN",
                },
            )

        severity, anomaly_type, threshold_value, description = self._classify(
            value=value,
            critical_high=critical_high,
            warning_high=warning_high,
            critical_low=critical_low,
            warning_low=warning_low,
            sensor_id=reading.get("sensor_id", "unknown"),
        )

        detected_at = int(time.time() * 1000)

        reading["anomaly"] = {
            "severity": severity,
            "type": anomaly_type,
            "threshold_value": threshold_value,
            "description": description,
            "detected_at": detected_at,
            "detector": "ThresholdDetector",
        }

        attributes = {
            "anomaly.severity": severity,
            "anomaly.type": anomaly_type,
            "anomaly.threshold_value": str(threshold_value) if threshold_value is not None else "",
            "anomaly.detected_at": str(detected_at),
            "anomaly.detector": "ThresholdDetector",
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

    @staticmethod
    def _classify(
        value: float,
        critical_high: float,
        warning_high: float,
        critical_low: float,
        warning_low: float,
        sensor_id: str,
    ) -> tuple[str, str, float | None, str]:
        """Classify a sensor value against threshold boundaries.

        Evaluation order matters: critical conditions are checked first to ensure
        the highest severity is always reported.

        Args:
            value: The sensor reading value.
            critical_high: Upper critical limit.
            warning_high: Upper warning limit.
            critical_low: Lower critical limit.
            warning_low: Lower warning limit.
            sensor_id: Sensor identifier for description text.

        Returns:
            Tuple of (severity, anomaly_type, threshold_value, description).
        """
        if value >= critical_high:
            return (
                "CRITICAL",
                "threshold_exceeded",
                critical_high,
                f"Sensor {sensor_id}: value {value} >= critical high {critical_high}",
            )

        if value <= critical_low:
            return (
                "CRITICAL",
                "threshold_exceeded",
                critical_low,
                f"Sensor {sensor_id}: value {value} <= critical low {critical_low}",
            )

        if value >= warning_high:
            return (
                "WARNING",
                "threshold_warning",
                warning_high,
                f"Sensor {sensor_id}: value {value} >= warning high {warning_high}",
            )

        if value <= warning_low:
            return (
                "WARNING",
                "threshold_warning",
                warning_low,
                f"Sensor {sensor_id}: value {value} <= warning low {warning_low}",
            )

        return (
            "NORMAL",
            "none",
            None,
            f"Sensor {sensor_id}: value {value} within normal range",
        )
