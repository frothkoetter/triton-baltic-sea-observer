"""
Dynamic target tracking for NBA mission simulation.

Resolves moving target positions, reapplies intercept routes for surface units,
and places buoy formations relative to contact bearing (NATO MDA playbook).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from baltic_ship_routing import SIMULATION_HOURS_PER_TICK, build_sea_route, move_towards_on_water


EARTH_RADIUS_M = 6_371_000


def _deg_to_rad(d: float) -> float:
    return d * math.pi / 180.0


def _rad_to_deg(r: float) -> float:
    return r * 180.0 / math.pi


def destination_point(lat: float, lon: float, bearing_deg: float, distance_km: float) -> tuple[float, float]:
    """Spherical forward geodesic (short distances, Baltic-scale)."""
    br = _deg_to_rad(bearing_deg)
    lat1 = _deg_to_rad(lat)
    lon1 = _deg_to_rad(lon)
    ang = distance_km * 1000.0 / EARTH_RADIUS_M
    lat2 = math.asin(
        math.sin(lat1) * math.cos(ang) + math.cos(lat1) * math.sin(ang) * math.cos(br)
    )
    lon2 = lon1 + math.atan2(
        math.sin(br) * math.sin(ang) * math.cos(lat1),
        math.cos(ang) - math.sin(lat1) * math.sin(lat2),
    )
    return (_rad_to_deg(lat2), _rad_to_deg(lon2))


@dataclass
class TrackSample:
    latitude: float
    longitude: float
    cog_deg: float | None = None
    sog_kn: float | None = None
    mmsi: str | None = None
    source: str = "static"


def advance_target_sample(sample: TrackSample, hours: float = SIMULATION_HOURS_PER_TICK) -> TrackSample:
    """Dead-reckoning for demo when AIS poll is unavailable."""
    if sample.sog_kn is None or sample.cog_deg is None or sample.sog_kn <= 0:
        return sample
    nm = sample.sog_kn * hours
    km = nm * 1.852
    lat, lon = destination_point(sample.latitude, sample.longitude, sample.cog_deg, km)
    return TrackSample(
        latitude=lat,
        longitude=lon,
        cog_deg=sample.cog_deg,
        sog_kn=sample.sog_kn,
        mmsi=sample.mmsi,
        source="dead_reckon",
    )


def formation_slots(
    center: TrackSample,
    formation_key: str,
    count: int,
    formation_spec: dict[str, Any],
) -> list[tuple[float, float, str]]:
    """
  Returns (lat, lon, role_tag) for buoy deployment relative to target.
  Uses contact COG for ahead/astern arcs when available.
  """
    spec = formation_spec.get(formation_key, {})
    min_km = float(spec.get("standoff_km_min", 5))
    max_km = float(spec.get("standoff_km_max", 12))
    arc = float(spec.get("arc_degrees", 360))
    offset = spec.get("offset_bearing", "omnidirectional")
    base_bearing = center.cog_deg if center.cog_deg is not None else 90.0

    if offset == "ahead":
        center_bearing = base_bearing
    elif offset == "astern":
        center_bearing = (base_bearing + 180) % 360
    else:
        center_bearing = 0.0

    half_arc = arc / 2.0
    slots: list[tuple[float, float, str]] = []
    if count <= 0:
        return slots
    step = arc / max(count - 1, 1) if count > 1 else 0.0
    for i in range(count):
        bearing = (center_bearing - half_arc + step * i) % 360
        dist_km = min_km + (max_km - min_km) * (i / max(count - 1, 1))
        lat, lon = destination_point(center.latitude, center.longitude, bearing, dist_km)
        slots.append((lat, lon, f"slot_{i + 1}"))
    return slots


def intercept_tick(
    ship_lat: float,
    ship_lon: float,
    target: TrackSample,
    speed_kn: float,
) -> tuple[float, float, list[dict[str, float]]]:
    """One simulation tick: move surface unit toward latest target on water."""
    waypoints = build_sea_route(
        ship_lon,
        ship_lat,
        target.longitude,
        target.latitude,
    )
    new_lat, new_lon = move_towards_on_water(
        ship_lat,
        ship_lon,
        target.latitude,
        target.longitude,
        speed_kn,
        hours=SIMULATION_HOURS_PER_TICK,
    )
    return new_lat, new_lon, waypoints


def track_should_continue(elapsed_min: float, track_duration_min: int) -> bool:
    return elapsed_min < track_duration_min


def format_stanag_mission_footer(template: str, mission_id: str, buoy_id: str, correlation_id: str) -> str:
    return template.format(
        mission_id=mission_id,
        buoy_id=buoy_id,
        correlation_id=correlation_id or "N/A",
    )


def tag_stanag_message_body(
    body: str,
    *,
    mission_id: str,
    buoy_id: str | None = None,
    correlation_id: str | None = None,
    template: str = "// EINSATZ {mission_id} BUOY {buoy_id} CORR {correlation_id}",
) -> str:
    footer = format_stanag_mission_footer(
        template,
        mission_id,
        buoy_id or "—",
        correlation_id or "—",
    )
    if footer.strip() in body:
        return body
    return f"{body.rstrip()}\n{footer}\n"