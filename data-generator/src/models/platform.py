"""Platform definitions for offshore Oil & Gas installations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Platform:
    """Represents a single offshore Oil & Gas platform."""

    platform_id: str
    platform_name: str
    platform_type: str
    latitude: float
    longitude: float
    region: str
    timezone: str


PLATFORMS: dict[str, Platform] = {
    "ALPHA": Platform(
        platform_id="ALPHA",
        platform_name="Alpha Station",
        platform_type="Deep-water Drilling",
        latitude=28.5,
        longitude=-88.5,
        region="Gulf of Mexico",
        timezone="America/Chicago",
    ),
    "BRAVO": Platform(
        platform_id="BRAVO",
        platform_name="Bravo Platform",
        platform_type="Production & Separation",
        latitude=57.5,
        longitude=1.5,
        region="North Sea",
        timezone="Europe/London",
    ),
    "CHARLIE": Platform(
        platform_id="CHARLIE",
        platform_name="Charlie FPSO",
        platform_type="Floating Production",
        latitude=-25.0,
        longitude=-43.0,
        region="Santos Basin",
        timezone="America/Sao_Paulo",
    ),
    "DELTA": Platform(
        platform_id="DELTA",
        platform_name="Delta Jack-up",
        platform_type="Shallow Water",
        latitude=26.5,
        longitude=51.5,
        region="Persian Gulf",
        timezone="Asia/Qatar",
    ),
    "ECHO": Platform(
        platform_id="ECHO",
        platform_name="Echo Semi-sub",
        platform_type="Semi-submersible",
        latitude=-22.5,
        longitude=-40.0,
        region="Campos Basin",
        timezone="America/Sao_Paulo",
    ),
}
