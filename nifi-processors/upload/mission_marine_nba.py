"""Apply NBA intercept ticks to German Navy simulator state for parallel missions."""

from __future__ import annotations

from typing import Any

from mission_parallel import MissionRuntimeStore
from mission_target_track import TrackSample, intercept_tick


def apply_nba_intercept_ticks(
    ships: list[dict[str, Any]],
    store: MissionRuntimeStore,
) -> list[dict[str, Any]]:
    by_mmsi: dict[str, Any] = {}
    for rec in store.active_missions():
        for mmsi in rec.surface_mmsis():
            by_mmsi[mmsi] = rec

    for ship in ships:
        mmsi = str(ship.get("MMSI", ""))
        rec = by_mmsi.get(mmsi)
        if not rec:
            continue

        surface = (rec.parameters_json or {}).get("surface") or {}
        speed = float(surface.get("speed_cap_kn") or 18.0)
        target = TrackSample(
            latitude=rec.target_latitude,
            longitude=rec.target_longitude,
            cog_deg=None,
            sog_kn=None,
            mmsi=rec.target_mmsi,
        )
        lat = float(ship.get("Latitude", 0))
        lon = float(ship.get("Longitude", 0))
        new_lat, new_lon, waypoints = intercept_tick(lat, lon, target, speed)
        ship["Latitude"] = new_lat
        ship["Longitude"] = new_lon
        ship["Phase"] = "nba_intercept"
        ship["activeMissionId"] = rec.mission_id
        mids = ship.get("activeMissionIds") or []
        if rec.mission_id not in mids:
            mids = [*mids, rec.mission_id]
        ship["activeMissionIds"] = mids
        if waypoints:
            ship["RouteWaypoints"] = [{"lat": w["lat"], "lon": w["lon"]} for w in waypoints[:12]]
            ship["RouteIndex"] = 0
        ship["MissionTicksRemaining"] = max(int(ship.get("MissionTicksRemaining") or 0), 24)
    return ships