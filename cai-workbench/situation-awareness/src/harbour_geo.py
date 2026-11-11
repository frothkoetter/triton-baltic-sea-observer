"""Geographic reference points for Baltic harbours (sourced from cdw-analyse harbour DDL)."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

# Coordinates from ``baltic_sea_harbours`` INSERT in create_db_tables_trino.sql
BALTIC_SEA_HARBOURS: dict[str, tuple[float, float]] = {
    "Stockholm": (59.3293, 18.0686),
    "Helsinki": (60.1695, 24.9354),
    "Tallinn": (59.4370, 24.7536),
    "Riga": (56.9496, 24.1052),
    "Gdynia": (54.5189, 18.5305),
    "Klaipėda": (55.7033, 21.1443),
    "Turku": (60.4518, 22.2666),
    "Mariehamn": (60.0973, 19.9348),
    "Liepāja": (56.5110, 21.0136),
    "Ventspils": (57.3890, 21.5610),
    "Kaliningrad": (54.7104, 20.4522),
    "Świnoujście": (53.9106, 14.2478),
    "Rostock": (54.0887, 12.1405),
    "Travemünde": (53.9624, 10.8672),
    "St. Petersburg": (59.9343, 30.3351),
    "Karlskrona": (56.1612, 15.5869),
    "Kiel": (54.3233, 10.1228),
    "Wismar": (53.8934, 11.4536),
    "Stralsund": (54.3091, 13.0810),
    "Sassnitz": (54.5183, 13.6414),
    "Greifswald": (54.0934, 13.3781),
    "Nynäshamn": (58.9036, 17.9470),
    "Ustka": (54.5801, 16.8596),
    "Pori": (61.4847, 21.7976),
    "Kemi": (65.7369, 24.5636),
    "Gdańsk": (54.3520, 18.6466),
    "Paldiski": (59.3567, 24.0539),
    "Rønne": (55.1037, 14.7065),
    "Visby": (57.6409, 18.2960),
    "Bolderāja": (56.9950, 24.0500),
    "Primorsk": (60.3565, 28.6094),
}

GERMAN_BALTIC_HARBOURS: dict[str, tuple[float, float]] = {
    k: v
    for k, v in BALTIC_SEA_HARBOURS.items()
    if k
    in {
        "Kiel",
        "Greifswald",
        "Rostock",
        "Wismar",
        "Stralsund",
        "Sassnitz",
        "Travemünde",
    }
}


def _fold_name(name: str) -> str:
    folded = unicodedata.normalize("NFKD", name.strip())
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = re.sub(r"[^a-z0-9]+", "", folded.lower())
    return folded


_FOLDED_TO_CANONICAL: dict[str, str] = {
    _fold_name(k): k for k in BALTIC_SEA_HARBOURS
}
_FOLDED_TO_CANONICAL.setdefault("gdansk", "Gdańsk")
_FOLDED_TO_CANONICAL.setdefault("gdask", "Gdańsk")
_FOLDED_TO_CANONICAL.setdefault("klaipeda", "Klaipėda")
_FOLDED_TO_CANONICAL.setdefault("swinoujscie", "Świnoujście")
_FOLDED_TO_CANONICAL.setdefault("stpetersburg", "St. Petersburg")
_FOLDED_TO_CANONICAL.setdefault("saintpetersburg", "St. Petersburg")


@dataclass(frozen=True)
class HarbourPoint:
    name: str
    latitude: float
    longitude: float


def harbour_points() -> list[HarbourPoint]:
    return [
        HarbourPoint(name=n, latitude=lat, longitude=lon)
        for n, (lat, lon) in sorted(BALTIC_SEA_HARBOURS.items())
    ]


def canonical_harbour_name(name: str) -> str | None:
    if not name or (isinstance(name, float) and name != name):
        return None
    key = str(name).strip()
    if key in BALTIC_SEA_HARBOURS:
        return key
    folded = _fold_name(key)
    return _FOLDED_TO_CANONICAL.get(folded)


def coords_for_harbour(name: str) -> tuple[float, float] | None:
    if not name:
        return None
    canon = canonical_harbour_name(name)
    if canon:
        return BALTIC_SEA_HARBOURS[canon]
    return None
