"""CLI entrypoint for the Oil & Gas sensor data generator.

Continuously produces simulated sensor readings for 5 offshore platforms
(~250 sensors total) and publishes them via MQTT, REST, or both.

Usage::

    python -m src.main --broker mosquitto --port 1883 --interval 2 --mode both
"""

from __future__ import annotations

import logging
import signal
import sys
import time
import uuid
from collections.abc import Sequence

import click

from src.config import GeneratorConfig
from src.generators.mqtt_publisher import MQTTPublisher
from src.generators.rest_publisher import RESTPublisher
from src.models.platform import PLATFORMS
from src.models.reading import SensorReading
from src.models.sensor import Sensor, generate_sensors_for_platform
from src.patterns.signal import generate_reading

logger = logging.getLogger(__name__)

# ── Shared state for graceful shutdown ──────────────────────────────────

_shutdown_requested = False


def _handle_sigint(signum: int, frame: object) -> None:
    global _shutdown_requested  # noqa: PLW0603
    _shutdown_requested = True
    logger.info("Shutdown requested (SIGINT) -- finishing current cycle")


# ── Stats tracker ───────────────────────────────────────────────────────


class _Stats:
    """Accumulates runtime statistics and logs them periodically."""

    def __init__(self, report_interval: float = 30.0) -> None:
        self.total_readings = 0
        self.total_anomalies = 0
        self.mqtt_ok = 0
        self.mqtt_fail = 0
        self.rest_ok = 0
        self.rest_fail = 0
        self._last_report = time.monotonic()
        self._report_interval = report_interval

    def maybe_report(self) -> None:
        now = time.monotonic()
        if now - self._last_report >= self._report_interval:
            elapsed = now - self._last_report
            rps = self.total_readings / max(elapsed, 0.001)
            logger.info(
                "STATS | readings=%d | anomalies=%d | rps=%.1f | "
                "mqtt_ok=%d mqtt_fail=%d | rest_ok=%d rest_fail=%d",
                self.total_readings,
                self.total_anomalies,
                rps,
                self.mqtt_ok,
                self.mqtt_fail,
                self.rest_ok,
                self.rest_fail,
            )
            # Reset counters for next window
            self.total_readings = 0
            self.total_anomalies = 0
            self.mqtt_ok = 0
            self.mqtt_fail = 0
            self.rest_ok = 0
            self.rest_fail = 0
            self._last_report = now


# ── Core generation loop ────────────────────────────────────────────────


def _select_pattern(anomaly_prob: float, rng: object) -> tuple[str, str]:
    """Choose a signal pattern and quality flag based on anomaly probability.

    Returns (pattern_name, quality_flag).
    """
    import numpy as np

    rng_local = np.random.default_rng() if rng is None else rng
    if float(rng_local.random()) < anomaly_prob:
        pattern = "degradation" if float(rng_local.random()) < 0.6 else "failure"
        quality = "SUSPECT" if pattern == "degradation" else "BAD"
        return pattern, quality
    return "normal", "GOOD"


def _generate_cycle(
    sensors_by_platform: dict[str, list[Sensor]],
    t_hours: float,
    anomaly_prob: float,
) -> tuple[list[SensorReading], int]:
    """Generate one reading per sensor across all active platforms.

    Returns (readings, anomaly_count).
    """
    import numpy as np

    rng = np.random.default_rng()
    readings: list[SensorReading] = []
    anomaly_count = 0
    now_ms = int(time.time() * 1000)

    for platform_id, sensors in sensors_by_platform.items():
        for sensor in sensors:
            pattern, quality = _select_pattern(anomaly_prob, rng)
            if pattern != "normal":
                anomaly_count += 1

            value = generate_reading(sensor, t_hours, pattern=pattern)

            readings.append(
                SensorReading(
                    reading_id=str(uuid.uuid4()),
                    platform_id=platform_id,
                    sensor_id=sensor.sensor_id,
                    sensor_type=sensor.sensor_type.value,
                    value=round(value, 4),
                    unit=sensor.unit,
                    timestamp=now_ms,
                    quality_flag=quality,
                )
            )

    return readings, anomaly_count


def _publish_readings(
    readings: Sequence[SensorReading],
    mqtt_pub: MQTTPublisher | None,
    rest_pub: RESTPublisher | None,
    stats: _Stats,
) -> None:
    """Fan out readings to the active publishers."""
    for reading in readings:
        if mqtt_pub is not None:
            ok = mqtt_pub.publish(reading)
            if ok:
                stats.mqtt_ok += 1
            else:
                stats.mqtt_fail += 1

        if rest_pub is not None:
            ok = rest_pub.publish(reading)
            if ok:
                stats.rest_ok += 1
            else:
                stats.rest_fail += 1


# ── CLI definition ──────────────────────────────────────────────────────


@click.command("oilgas-generator")
@click.option(
    "--broker",
    default=None,
    help="MQTT broker hostname (overrides MQTT_BROKER env var).",
)
@click.option(
    "--port",
    default=None,
    type=int,
    help="MQTT broker port (overrides MQTT_PORT env var).",
)
@click.option(
    "--interval",
    default=None,
    type=int,
    help="Seconds between generation cycles (overrides INTERVAL_SECONDS env var).",
)
@click.option(
    "--platforms",
    "platform_filter",
    default=None,
    help="Comma-separated platform IDs to activate (overrides PLATFORMS env var).",
)
@click.option(
    "--anomaly-prob",
    default=None,
    type=float,
    help="Probability [0..1] of injecting anomalies (overrides ANOMALY_PROBABILITY env var).",
)
@click.option(
    "--mode",
    type=click.Choice(["mqtt", "rest", "both"], case_sensitive=False),
    default="both",
    show_default=True,
    help="Publishing mode.",
)
@click.option(
    "--nifi-url",
    default=None,
    help="NiFi ListenHTTP URL (overrides NIFI_HTTPS_URL env var).",
)
def main(
    broker: str | None,
    port: int | None,
    interval: int | None,
    platform_filter: str | None,
    anomaly_prob: float | None,
    mode: str,
    nifi_url: str | None,
) -> None:
    """Oil & Gas sensor data generator for the NiFi monitoring platform.

    Simulates 5 offshore platforms with ~250 sensors publishing real-time
    readings via MQTT and/or REST at a configurable cadence.
    """
    # ── Configuration ───────────────────────────────────────────────────
    config = GeneratorConfig.from_env()
    config.configure_logging()

    # CLI overrides take precedence over env vars
    broker_host = broker or config.mqtt_broker
    broker_port = port or config.mqtt_port
    interval_sec = interval or config.interval_seconds
    anomaly_probability = anomaly_prob if anomaly_prob is not None else config.anomaly_probability
    nifi_endpoint = nifi_url or config.nifi_http_url

    if platform_filter:
        active_ids = [p.strip().upper() for p in platform_filter.split(",") if p.strip()]
    else:
        active_ids = config.platforms

    # ── Validate platforms ──────────────────────────────────────────────
    for pid in active_ids:
        if pid not in PLATFORMS:
            logger.error("Unknown platform: %s (valid: %s)", pid, list(PLATFORMS.keys()))
            sys.exit(1)

    # ── Build sensor inventories ────────────────────────────────────────
    sensors_by_platform: dict[str, list[Sensor]] = {}
    total_sensors = 0
    for pid in active_ids:
        sensors = generate_sensors_for_platform(pid)
        sensors_by_platform[pid] = sensors
        total_sensors += len(sensors)

    logger.info(
        "Initialized %d platforms, %d sensors total | interval=%ds | anomaly_prob=%.2f | mode=%s",
        len(active_ids),
        total_sensors,
        interval_sec,
        anomaly_probability,
        mode,
    )

    # ── Set up publishers ───────────────────────────────────────────────
    mqtt_pub: MQTTPublisher | None = None
    rest_pub: RESTPublisher | None = None

    if mode in ("mqtt", "both"):
        mqtt_pub = MQTTPublisher(
            broker_host=broker_host,
            broker_port=broker_port,
        )
        mqtt_pub.connect()

    if mode in ("rest", "both"):
        rest_pub = RESTPublisher(nifi_url=nifi_endpoint)

    # ── Register signal handler ─────────────────────────────────────────
    signal.signal(signal.SIGINT, _handle_sigint)

    # ── Main generation loop ────────────────────────────────────────────
    stats = _Stats(report_interval=30.0)
    start_time = time.monotonic()

    logger.info("Generator started -- press Ctrl+C to stop")

    try:
        while not _shutdown_requested:
            cycle_start = time.monotonic()
            t_hours = (cycle_start - start_time) / 3600.0

            readings, anomalies = _generate_cycle(
                sensors_by_platform,
                t_hours,
                anomaly_probability,
            )

            _publish_readings(readings, mqtt_pub, rest_pub, stats)

            stats.total_readings += len(readings)
            stats.total_anomalies += anomalies
            stats.maybe_report()

            # Sleep for the remainder of the interval
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0.0, interval_sec - elapsed)
            if sleep_time > 0 and not _shutdown_requested:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Shutting down publishers ...")
        if mqtt_pub is not None:
            mqtt_pub.disconnect()
        if rest_pub is not None:
            rest_pub.close()
        logger.info("Generator stopped")


if __name__ == "__main__":
    main()
