"""Geographic reference points for Baltic harbours (sourced from cdw-analyse harbour DDL)."""

from __future__ import annotations

from dataclasses import dataclass

# Coordinates from baltic_sea_harbours INSERT in create_db_tables_hive.sql
GERMAN_BALTIC_HARBOURS: dict[str, tuple[float, float]] = {
    "Kiel": (54.3233, 10.1228),
    "Greifswald": (54.0934, 13.3781),
    "Rostock": (54.0887, 12.1405),
    "Wismar": (53.8934, 11.4536),
    "Stralsund": (54.3091, 13.0810),
    "Sassnitz": (54.5183, 13.6414),
    "Travemünde": (53.9624, 10.8672),
}


@dataclass(frozen=True)
class HarbourPoint:
    name: str
    latitude: float
    longitude: float


def harbour_points() -> list[HarbourPoint]:
    return [
        HarbourPoint(name=n, latitude=lat, longitude=lon)
        for n, (lat, lon) in sorted(GERMAN_BALTIC_HARBOURS.items())
    ]


def coords_for_harbour(name: str) -> tuple[float, float] | None:
    if not name:
        return None
    key = name.strip()
    if key in GERMAN_BALTIC_HARBOURS:
        return GERMAN_BALTIC_HARBOURS[key]
    lower = {k.lower(): v for k, v in GERMAN_BALTIC_HARBOURS.items()}
    return lower.get(key.lower())
