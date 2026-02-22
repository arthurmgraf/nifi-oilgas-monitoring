"""MQTT publisher for streaming sensor readings to an MQTT broker."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import paho.mqtt.client as mqtt

if TYPE_CHECKING:
    from src.models.reading import SensorReading

logger = logging.getLogger(__name__)


class MQTTPublisher:
    """Publishes sensor readings to an MQTT broker using MQTTv5.

    Topic pattern: ``sensors/{platform_id}/{sensor_id}/data``
    """

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        client_id: str = "oilgas-data-generator",
    ) -> None:
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._client_id = client_id
        self._client: mqtt.Client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        self._connected = False

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        rc: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        if rc == mqtt.ReasonCode(mqtt.CONNACK_ACCEPTED):
            self._connected = True
            logger.info(
                "MQTT connected to %s:%d (client=%s)",
                self._broker_host,
                self._broker_port,
                self._client_id,
            )
        else:
            logger.error("MQTT connection refused: %s", rc)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.DisconnectFlags,
        rc: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        self._connected = False
        logger.warning("MQTT disconnected (rc=%s)", rc)

    def connect(self) -> None:
        """Establish connection to the MQTT broker."""
        try:
            self._client.connect(self._broker_host, self._broker_port)
            self._client.loop_start()
            logger.info(
                "MQTT connecting to %s:%d ...",
                self._broker_host,
                self._broker_port,
            )
        except OSError:
            logger.exception(
                "Failed to connect to MQTT broker at %s:%d",
                self._broker_host,
                self._broker_port,
            )

    def publish(self, reading: SensorReading) -> bool:
        """Publish a single sensor reading.

        Returns True on successful enqueue, False on failure.
        """
        topic = f"sensors/{reading.platform_id}/{reading.sensor_id}/data"
        payload = reading.to_json()

        try:
            info = self._client.publish(topic, payload, qos=1)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.warning(
                    "MQTT publish failed for %s (rc=%s)",
                    reading.sensor_id,
                    info.rc,
                )
                return False
            return True
        except Exception:
            logger.exception("MQTT publish error for sensor %s", reading.sensor_id)
            return False

    def disconnect(self) -> None:
        """Gracefully disconnect from the broker."""
        try:
            self._client.loop_stop()
            self._client.disconnect()
            logger.info("MQTT publisher disconnected")
        except Exception:
            logger.exception("Error during MQTT disconnect")

    @property
    def is_connected(self) -> bool:
        return self._connected
