"""Centralized configuration loaded from environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GeneratorConfig:
    """Configuration for the Oil & Gas data generator.

    All values are loaded from environment variables with sensible defaults
    for local development. Override via env vars in Docker / K8s deployments.
    """

    mqtt_broker: str = "localhost"
    mqtt_port: int = 1883
    nifi_http_url: str = "http://localhost:8080"
    platforms: list[str] = field(
        default_factory=lambda: ["ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"]
    )
    interval_seconds: int = 2
    anomaly_probability: float = 0.05
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> GeneratorConfig:
        """Build configuration from environment variables."""
        platforms_raw = os.environ.get("PLATFORMS", "ALPHA,BRAVO,CHARLIE,DELTA,ECHO")
        platforms = [p.strip() for p in platforms_raw.split(",") if p.strip()]

        return cls(
            mqtt_broker=os.environ.get("MQTT_BROKER", "localhost"),
            mqtt_port=int(os.environ.get("MQTT_PORT", "1883")),
            nifi_http_url=os.environ.get("NIFI_HTTP_URL", "http://localhost:8080"),
            platforms=platforms,
            interval_seconds=int(os.environ.get("INTERVAL_SECONDS", "2")),
            anomaly_probability=float(os.environ.get("ANOMALY_PROBABILITY", "0.05")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )

    def configure_logging(self) -> None:
        """Set up structured logging based on configured level."""
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper(), logging.INFO),
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
