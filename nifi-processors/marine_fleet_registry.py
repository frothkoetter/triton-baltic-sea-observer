"""Shared German naval fleet registry and MarineShipSimulator state access."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, TypedDict


class FleetUnit(TypedDict):
    class_: str  # noqa: A003 — stored as "class" in JSON-like dicts
    name: str
    mmsi: str


MARINESHIP_STATE_FILE = "/tmp/marineship_state.json"

GERMAN_NAVAL_FLEET: List[Dict[str, str]] = [{'class': 'Fregatten', 'name': 'FGS Brandenburg', 'mmsi': 'MAR123400'}, {'class': 'Fregatten', 'name': 'FGS Schleswig-Holstein', 'mmsi': 'MAR123401'}, {'class': 'Fregatten', 'name': 'FGS Bayern', 'mmsi': 'MAR123402'}, {'class': 'Fregatten', 'name': 'FGS Mecklenburg-Vorpommern', 'mmsi': 'MAR123403'}, {'class': 'Fregatten', 'name': 'FGS Sachsen', 'mmsi': 'MAR123404'}, {'class': 'Fregatten', 'name': 'FGS Hamburg', 'mmsi': 'MAR123405'}, {'class': 'Fregatten', 'name': 'FGS Hessen', 'mmsi': 'MAR123406'}, {'class': 'Fregatten', 'name': 'FGS Baden-Württemberg', 'mmsi': 'MAR123407'}, {'class': 'Fregatten', 'name': 'FGS Nordrhein-Westfalen', 'mmsi': 'MAR123408'}, {'class': 'Fregatten', 'name': 'FGS Sachsen-Anhalt', 'mmsi': 'MAR123409'}, {'class': 'Fregatten', 'name': 'FGS Rheinland-Pfalz', 'mmsi': 'MAR123410'}, {'class': 'Korvetten', 'name': 'FGS Braunschweig', 'mmsi': 'MAR123411'}, {'class': 'Korvetten', 'name': 'FGS Magdeburg', 'mmsi': 'MAR123412'}, {'class': 'Korvetten', 'name': 'FGS Erfurt', 'mmsi': 'MAR123413'}, {'class': 'Korvetten', 'name': 'FGS Oldenburg', 'mmsi': 'MAR123414'}, {'class': 'Korvetten', 'name': 'FGS Ludwigshafen am Rhein', 'mmsi': 'MAR123415'}, {'class': 'U-Boote', 'name': 'U-31', 'mmsi': 'MAR123416'}, {'class': 'U-Boote', 'name': 'U-32', 'mmsi': 'MAR123417'}, {'class': 'U-Boote', 'name': 'U-33', 'mmsi': 'MAR123418'}, {'class': 'U-Boote', 'name': 'U-34', 'mmsi': 'MAR123419'}, {'class': 'U-Boote', 'name': 'U-35', 'mmsi': 'MAR123420'}, {'class': 'U-Boote', 'name': 'U-36', 'mmsi': 'MAR123421'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Fulda', 'mmsi': 'MAR123422'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Weilheim', 'mmsi': 'MAR123423'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Sulzbach-Rosenberg', 'mmsi': 'MAR123424'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Dillingen', 'mmsi': 'MAR123425'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Homburg', 'mmsi': 'MAR123426'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Siegburg', 'mmsi': 'MAR123427'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Auerbach/Oberpfalz', 'mmsi': 'MAR123428'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Pegnitz', 'mmsi': 'MAR123429'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Passau', 'mmsi': 'MAR123430'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Weiden', 'mmsi': 'MAR123431'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Ensdorf', 'mmsi': 'MAR123432'}, {'class': 'Minenabwehreinheiten', 'name': 'FGS Kühlungsborn', 'mmsi': 'MAR123433'}, {'class': 'Flottendienstboote', 'name': 'A52 Oste', 'mmsi': 'MAR123434'}, {'class': 'Flottendienstboote', 'name': 'A50 Oker', 'mmsi': 'MAR123435'}, {'class': 'Flottendienstboote', 'name': 'A53 Alster', 'mmsi': 'MAR123436'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A1411 Berlin', 'mmsi': 'MAR123437'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A1412 Frankfurt am Main', 'mmsi': 'MAR123438'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A1413 Bonn', 'mmsi': 'MAR123439'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A511 Elbe', 'mmsi': 'MAR123440'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A512 Mosel', 'mmsi': 'MAR123441'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A513 Rhein', 'mmsi': 'MAR123442'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A514 Werra', 'mmsi': 'MAR123443'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A515 Main', 'mmsi': 'MAR123444'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A516 Donau', 'mmsi': 'MAR123445'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A1442 Spessart', 'mmsi': 'MAR123446'}, {'class': 'Versorgungsschiffe/Tender', 'name': 'A1443 Rhön', 'mmsi': 'MAR123447'}, {'class': 'Sonstige', 'name': 'FGS Gorch Fock', 'mmsi': 'MAR123448'}, {'class': 'Sonstige', 'name': 'FGS Planet', 'mmsi': 'MAR123449'}, {'class': 'Sonstige', 'name': 'FGS Wangerooge', 'mmsi': 'MAR123450'}, {'class': 'Sonstige', 'name': 'FGS Baltrum', 'mmsi': 'MAR123451'}, {'class': 'Sonstige', 'name': 'FGS Borkum', 'mmsi': 'MAR123452'}, {'class': 'Sonstige', 'name': 'FGS Norderney', 'mmsi': 'MAR123453'}, {'class': 'Sonstige', 'name': 'FGS Juist', 'mmsi': 'MAR123454'}, {'class': 'Sonstige', 'name': 'FGS Langeoog', 'mmsi': 'MAR123455'}]

UNIT_BY_MMSI: Dict[str, Dict[str, str]] = {u["mmsi"]: u for u in GERMAN_NAVAL_FLEET}
NAME_BY_MMSI: Dict[str, str] = {u["mmsi"]: u["name"] for u in GERMAN_NAVAL_FLEET}


def fleet_unit_for_index(index: int) -> Dict[str, str]:
    return GERMAN_NAVAL_FLEET[index % len(GERMAN_NAVAL_FLEET)]


def load_simulated_ship_positions() -> List[Dict[str, Any]]:
    """Positions from MarineShipSimulator state (same node ``/tmp`` file)."""
    if not os.path.exists(MARINESHIP_STATE_FILE):
        return []
    try:
        with open(MARINESHIP_STATE_FILE, "r", encoding="utf-8") as f:
            ships = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(ships, list):
        return []
    out: List[Dict[str, Any]] = []
    for ship in ships:
        if not isinstance(ship, dict):
            continue
        mmsi = ship.get("MMSI")
        if not mmsi:
            continue
        try:
            lat = float(ship["Latitude"])
            lon = float(ship["Longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        name = ship.get("Name") or NAME_BY_MMSI.get(mmsi, str(mmsi))
        out.append({"name": name, "mmsi": mmsi, "lat": lat, "lon": lon})
    return out
