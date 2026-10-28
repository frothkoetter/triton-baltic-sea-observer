"""Paths and helpers for AIS / marine ship simulator /tmp JSON state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Sequence

from marine_fleet_registry import MARINESHIP_STATE_FILE

AIS_SHIP_STATE_FILE = "/tmp/ship_state.json"

ROUTE_STATE_KEYS = ("RouteWaypoints", "RouteIndex")

ResetMode = Literal["delete_files", "clear_route_fields"]


@dataclass
class RouteStateResetResult:
    """Outcome of a route-state reset on one or more /tmp state files."""

    mode: ResetMode = "delete_files"
    deleted_files: List[str] = field(default_factory=list)
    updated_files: List[str] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    ships_route_cleared: int = 0
    errors: List[str] = field(default_factory=list)
    # Raw JSON read from each target path immediately before reset (None if file absent).
    snapshots: Dict[str, Any] = field(default_factory=dict)


def _state_paths(reset_ais: bool, reset_marine: bool) -> List[str]:
    paths: List[str] = []
    if reset_ais:
        paths.append(AIS_SHIP_STATE_FILE)
    if reset_marine:
        paths.append(MARINESHIP_STATE_FILE)
    return paths


def _read_state_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _snapshot_state_file(path: str, result: RouteStateResetResult) -> None:
    if not os.path.exists(path):
        result.snapshots[path] = None
        return
    try:
        result.snapshots[path] = _read_state_file(path)
    except (OSError, json.JSONDecodeError) as exc:
        result.snapshots[path] = {"_read_error": str(exc)}


def _clear_route_fields_on_ship(ship: Dict[str, Any]) -> bool:
    if not isinstance(ship, dict):
        return False
    changed = False
    if ship.get("RouteWaypoints"):
        ship["RouteWaypoints"] = []
        changed = True
    if ship.get("RouteIndex", 0) != 0:
        ship["RouteIndex"] = 0
        changed = True
    if "RouteWaypoints" not in ship:
        ship["RouteWaypoints"] = []
        changed = True
    if "RouteIndex" not in ship:
        ship["RouteIndex"] = 0
        changed = True
    return changed


def reset_baltic_ship_route_state(
    *,
    reset_ais: bool = True,
    reset_marine: bool = True,
    mode: ResetMode = "delete_files",
) -> RouteStateResetResult:
    """
    Reset persisted route state used by ShipSimulationRoutesGemini and MarineShipSimulator.

    ``delete_files``: remove the JSON state files; simulators re-initialize on next run.
    ``clear_route_fields``: keep MMSI/position/destinations but empty ``RouteWaypoints`` /
    ``RouteIndex`` so the next tick recomputes water-only routes.
    """
    result = RouteStateResetResult(mode=mode)
    for path in _state_paths(reset_ais, reset_marine):
        _snapshot_state_file(path, result)
        if mode == "delete_files":
            if not os.path.exists(path):
                result.missing_files.append(path)
                continue
            try:
                os.remove(path)
                result.deleted_files.append(path)
            except OSError as exc:
                result.errors.append(f"{path}: {exc}")
            continue

        if not os.path.exists(path):
            result.missing_files.append(path)
            continue
        try:
            ships = result.snapshots.get(path)
            if not isinstance(ships, list):
                ships = _read_state_file(path)
        except (OSError, json.JSONDecodeError) as exc:
            result.errors.append(f"{path}: read failed: {exc}")
            continue
        if not isinstance(ships, list):
            result.errors.append(f"{path}: expected JSON array of ships")
            continue
        cleared = 0
        for ship in ships:
            if _clear_route_fields_on_ship(ship):
                cleared += 1
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(ships, f)
            result.updated_files.append(path)
            result.ships_route_cleared += cleared
        except OSError as exc:
            result.errors.append(f"{path}: write failed: {exc}")
    return result


def result_summary(
    result: RouteStateResetResult,
    *,
    include_snapshots: bool = False,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "mode": result.mode,
        "deleted_files": result.deleted_files,
        "updated_files": result.updated_files,
        "missing_files": result.missing_files,
        "ships_route_cleared": result.ships_route_cleared,
        "errors": result.errors,
    }
    if include_snapshots:
        summary["snapshots"] = result.snapshots
    return summary


def reset_payload(
    result: RouteStateResetResult,
    *,
    include_snapshots: bool = True,
) -> Dict[str, Any]:
    """JSON body for NiFi FlowFile content after a reset run."""
    body: Dict[str, Any] = {"reset": result_summary(result, include_snapshots=False)}
    if include_snapshots:
        body["snapshots"] = result.snapshots
    return body
