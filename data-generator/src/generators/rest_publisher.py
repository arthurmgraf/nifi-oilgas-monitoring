"""REST publisher for pushing sensor readings to NiFi HTTP endpoints."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import ReadTimeout, RequestException

if TYPE_CHECKING:
    from src.models.reading import SensorReading

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 5


class RESTPublisher:
    """Publishes sensor readings to a NiFi ListenHTTP processor via POST.

    Handles both single-reading and batch modes with resilient error
    handling so that transient failures do not halt the generator.
    """

    def __init__(self, nifi_url: str, verify_ssl: bool = False) -> None:
        self._nifi_url = nifi_url.rstrip("/")
        self._verify_ssl = verify_ssl
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._session.verify = verify_ssl

        if not verify_ssl:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def publish(self, reading: SensorReading) -> bool:
        """POST a single sensor reading to the NiFi endpoint.

        Returns True on HTTP 2xx, False on any failure.
        """
        try:
            response = self._session.post(
                self._nifi_url,
                data=reading.to_json(),
                timeout=_TIMEOUT_SECONDS,
            )
            if response.ok:
                return True

            logger.warning(
                "REST publish returned %d for sensor %s",
                response.status_code,
                reading.sensor_id,
            )
            return False

        except (ReqConnectionError, ReadTimeout):
            logger.warning(
                "REST endpoint unreachable at %s (sensor=%s)",
                self._nifi_url,
                reading.sensor_id,
            )
            return False
        except RequestException:
            logger.exception(
                "REST publish error for sensor %s",
                reading.sensor_id,
            )
            return False

    def publish_batch(self, readings: list[SensorReading]) -> int:
        """POST a batch of sensor readings as a JSON array.

        Returns the number of readings successfully delivered.
        """
        if not readings:
            return 0

        payload = "[" + ",".join(r.to_json() for r in readings) + "]"

        try:
            response = self._session.post(
                self._nifi_url,
                data=payload,
                timeout=_TIMEOUT_SECONDS,
            )
            if response.ok:
                return len(readings)

            logger.warning(
                "REST batch publish returned %d (%d readings)",
                response.status_code,
                len(readings),
            )
            return 0

        except (ReqConnectionError, ReadTimeout):
            logger.warning(
                "REST endpoint unreachable at %s (batch of %d)",
                self._nifi_url,
                len(readings),
            )
            return 0
        except RequestException:
            logger.exception("REST batch publish error (%d readings)", len(readings))
            return 0

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()
        logger.info("REST publisher session closed")
