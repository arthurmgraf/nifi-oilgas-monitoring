"""Equipment definitions for offshore Oil & Gas platforms.

Each platform has 10 pieces of equipment representing the core processing
units found on a typical offshore installation: compressors, separators,
pumps, turbines, heat exchangers, wellheads, risers, BOP stacks, flare
systems, and power generators.
"""

from __future__ import annotations

from dataclasses import dataclass

_EQUIPMENT_TEMPLATES: list[tuple[str, str, str, str, str]] = [
    ("COMP", "Gas Compressor", "Compressor", "Rolls-Royce", "RB211-6762"),
    ("SEP", "Production Separator", "Separator", "Schlumberger", "NATCO-HP-3P"),
    ("PUMP-INJ", "Water Injection Pump", "Pump", "Sulzer", "MSD-D-8S"),
    ("TURB", "Gas Turbine Generator", "Turbine", "Siemens", "SGT-400"),
    ("HEX", "Heat Exchanger", "Heat Exchanger", "Alfa Laval", "M15-BFG"),
    ("WH-01", "Wellhead Assembly A", "Wellhead", "Cameron", "DL-18-10K"),
    ("WH-02", "Wellhead Assembly B", "Wellhead", "FMC Technologies", "UWD-15K"),
    ("RISER", "Production Riser", "Riser", "Technip", "FCR-8IN"),
    ("BOP", "Blowout Preventer Stack", "BOP", "NOV", "Shaffer-18.75-15K"),
    ("FLARE", "Flare & Vent System", "Flare", "John Zink", "LRGO-60"),
]


@dataclass(frozen=True)
class Equipment:
    """A single piece of equipment installed on a platform."""

    equipment_id: str
    platform_id: str
    equipment_name: str
    equipment_type: str
    manufacturer: str
    model: str


def _build_equipment_registry() -> dict[str, Equipment]:
    """Generate equipment for all five platforms."""
    registry: dict[str, Equipment] = {}
    platform_ids = ["ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"]

    for pid in platform_ids:
        for suffix, name, eq_type, mfr, mdl in _EQUIPMENT_TEMPLATES:
            eq_id = f"{pid}-{suffix}"
            registry[eq_id] = Equipment(
                equipment_id=eq_id,
                platform_id=pid,
                equipment_name=f"{name} ({pid})",
                equipment_type=eq_type,
                manufacturer=mfr,
                model=mdl,
            )

    return registry


EQUIPMENT: dict[str, Equipment] = _build_equipment_registry()


def get_equipment_for_platform(platform_id: str) -> list[Equipment]:
    """Return all equipment belonging to a specific platform."""
    return [eq for eq in EQUIPMENT.values() if eq.platform_id == platform_id]
