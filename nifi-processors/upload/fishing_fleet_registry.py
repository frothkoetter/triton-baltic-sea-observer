"""German Baltic fishing fleet registry (ships table + AIS simulator)."""

from __future__ import annotations

from typing import Dict, List

FISHING_BOAT_STATE_FILE = "/tmp/fishing_boat_state.json"
SIMULATION_ROUTE_NAME = "german_fishing_tours"

# German Baltic harbours from defense.baltic_sea_harbours (country = Germany).
GERMAN_FISHING_HARBORS: Dict[str, Dict[str, float | str]] = {
    "Rostock": {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    "Travemünde": {"name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672},
    "Kiel": {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    "Wismar": {"name": "Wismar", "Latitude": 53.8934, "Longitude": 11.4536},
    "Stralsund": {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    "Sassnitz": {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    "Greifswald": {"name": "Greifswald", "Latitude": 54.0934, "Longitude": 13.3781},
}

GERMAN_FISHING_FLEET: List[Dict[str, str | float | int]] = [
    {"name": "FV Ostseekrabbe", "mmsi": "211871001", "home_port": "Rostock", "length_m": 26, "beam_m": 7.5, "year_built": 2004},
    {"name": "FV Warnemünder Dorsch", "mmsi": "211871002", "home_port": "Rostock", "length_m": 24, "beam_m": 7.0, "year_built": 2007},
    {"name": "FV Mecklenburger Scholle", "mmsi": "211871003", "home_port": "Rostock", "length_m": 30, "beam_m": 8.0, "year_built": 2011},
    {"name": "FV Travemünder Hummer", "mmsi": "211871004", "home_port": "Travemünde", "length_m": 22, "beam_m": 6.5, "year_built": 2003},
    {"name": "FV Lübecker Büsum", "mmsi": "211871005", "home_port": "Travemünde", "length_m": 28, "beam_m": 7.8, "year_built": 2009},
    {"name": "FV Kieler Sprotte", "mmsi": "211871006", "home_port": "Kiel", "length_m": 25, "beam_m": 7.2, "year_built": 2006},
    {"name": "FV Holsteiner Hecht", "mmsi": "211871007", "home_port": "Kiel", "length_m": 32, "beam_m": 8.5, "year_built": 2014},
    {"name": "FV Kieler Bucht", "mmsi": "211871008", "home_port": "Kiel", "length_m": 27, "beam_m": 7.6, "year_built": 2010},
    {"name": "FV Wismarer Hering", "mmsi": "211871009", "home_port": "Wismar", "length_m": 23, "beam_m": 6.8, "year_built": 2005},
    {"name": "FV Poeler Garn", "mmsi": "211871010", "home_port": "Wismar", "length_m": 29, "beam_m": 8.2, "year_built": 2012},
    {"name": "FV Rügenfischer", "mmsi": "211871011", "home_port": "Stralsund", "length_m": 26, "beam_m": 7.4, "year_built": 2008},
    {"name": "FV Strelasunder Hake", "mmsi": "211871012", "home_port": "Stralsund", "length_m": 24, "beam_m": 7.0, "year_built": 2006},
    {"name": "FV Sassnitzer Lachs", "mmsi": "211871013", "home_port": "Sassnitz", "length_m": 31, "beam_m": 8.3, "year_built": 2015},
    {"name": "FV Jasmunder Flunder", "mmsi": "211871014", "home_port": "Sassnitz", "length_m": 25, "beam_m": 7.1, "year_built": 2007},
    {"name": "FV Greifswalder Makrele", "mmsi": "211871015", "home_port": "Greifswald", "length_m": 27, "beam_m": 7.5, "year_built": 2009},
    {"name": "FV Peenestrom", "mmsi": "211871016", "home_port": "Greifswald", "length_m": 22, "beam_m": 6.6, "year_built": 2004},
    {"name": "FV Vorpommern", "mmsi": "211871017", "home_port": "Greifswald", "length_m": 28, "beam_m": 7.9, "year_built": 2011},
    {"name": "FV Küstenfischer Nord", "mmsi": "211871018", "home_port": "Rostock", "length_m": 24, "beam_m": 6.9, "year_built": 2005},
    {"name": "FV Fehmarnbelt", "mmsi": "211871019", "home_port": "Kiel", "length_m": 26, "beam_m": 7.3, "year_built": 2008},
    {"name": "FV Arkonasee", "mmsi": "211871020", "home_port": "Sassnitz", "length_m": 30, "beam_m": 8.1, "year_built": 2013},
]

UNIT_BY_MMSI: Dict[str, Dict[str, str | float | int]] = {u["mmsi"]: u for u in GERMAN_FISHING_FLEET}
NAME_BY_MMSI: Dict[str, str] = {u["mmsi"]: str(u["name"]) for u in GERMAN_FISHING_FLEET}


def fleet_unit_for_index(index: int) -> Dict[str, str | float | int]:
    return dict(GERMAN_FISHING_FLEET[index % len(GERMAN_FISHING_FLEET)])


def home_port_for_unit(unit: Dict[str, str | float | int]) -> Dict[str, float | str]:
    port = str(unit.get("home_port", "Rostock"))
    return dict(GERMAN_FISHING_HARBORS.get(port, GERMAN_FISHING_HARBORS["Rostock"]))
