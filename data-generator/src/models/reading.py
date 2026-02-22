"""Immutable sensor reading value object."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class SensorReading:
    """A single timestamped measurement from one sensor.

    Frozen and slotted for memory efficiency -- readings are produced at
    high volume and should not be mutated after creation.
    """

    reading_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    platform_id: str = ""
    sensor_id: str = ""
    sensor_type: str = ""
    value: float = 0.0
    unit: str = ""
    timestamp: int = 0  # epoch milliseconds
    quality_flag: str = "GOOD"

    def to_dict(self) -> dict[str, Any]:
        """Serialize the reading to a plain dictionary."""
        return {
            "reading_id": self.reading_id,
            "platform_id": self.platform_id,
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp,
            "quality_flag": self.quality_flag,
        }

    def to_json(self) -> str:
        """Serialize the reading to a JSON string."""
        return json.dumps(self.to_dict())
