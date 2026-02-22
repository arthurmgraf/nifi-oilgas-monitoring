"""CSV batch generator for writing sensor readings to disk.

Useful for historical back-fill scenarios or when NiFi ingests files
from a watched directory instead of receiving live MQTT / HTTP streams.
"""

from __future__ import annotations

import csv
import logging
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.sensor import Sensor

from src.models.reading import SensorReading

logger = logging.getLogger(__name__)

_CSV_COLUMNS = [
    "reading_id",
    "platform_id",
    "sensor_id",
    "sensor_type",
    "value",
    "unit",
    "timestamp",
    "quality_flag",
]


class CSVBatchGenerator:
    """Writes sensor readings to CSV files on disk."""

    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("CSV output directory: %s", self._output_dir.resolve())

    def generate_batch(self, readings: list[SensorReading], filename: str) -> Path:
        """Write a list of readings to a single CSV file.

        Returns the absolute path of the created file.
        """
        filepath = self._output_dir / filename
        with filepath.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            writer.writeheader()
            for reading in readings:
                writer.writerow(reading.to_dict())

        logger.info("Wrote %d readings to %s", len(readings), filepath)
        return filepath.resolve()

    def generate_historical(
        self,
        sensors: list[Sensor],
        platform_id: str,
        hours_back: int = 24,
        interval_seconds: int = 60,
    ) -> Path:
        """Generate a historical CSV spanning *hours_back* hours.

        Readings are produced at *interval_seconds* cadence for every
        sensor in the provided list.  The file is named with the
        platform id and a timestamp.
        """
        from src.patterns.signal import generate_reading as gen_value

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (hours_back * 3600 * 1000)
        step_ms = interval_seconds * 1000

        readings: list[SensorReading] = []
        ts = start_ms

        while ts <= now_ms:
            t_hours = (ts - start_ms) / (3600 * 1000)
            for sensor in sensors:
                value = gen_value(sensor, t_hours, pattern="normal")
                readings.append(
                    SensorReading(
                        reading_id=str(uuid.uuid4()),
                        platform_id=platform_id,
                        sensor_id=sensor.sensor_id,
                        sensor_type=sensor.sensor_type.value,
                        value=round(value, 4),
                        unit=sensor.unit,
                        timestamp=ts,
                        quality_flag="GOOD",
                    )
                )
            ts += step_ms

        filename = f"{platform_id}_historical_{int(time.time())}.csv"
        return self.generate_batch(readings, filename)
