"""Russian Baltic Fleet registry aligned with ``defense.rus_fleet`` CDW inserts."""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict

RUS_FLEET_STATE_FILE = "/tmp/rus_fleet_state.json"
SIMULATION_ROUTE_NAME = "rus_fleet_positions"

# Home bases (lat/lon WGS84) — names match SQL ``home_base`` column.
RUS_HOME_BASES: Dict[str, Dict[str, float | str]] = {
    "Baltiysk": {"name": "Baltiysk", "Latitude": 54.64, "Longitude": 19.82},
    "Kronstadt": {"name": "Kronstadt", "Latitude": 59.99, "Longitude": 29.77},
    "Primorsk": {"name": "Primorsk", "Latitude": 60.34, "Longitude": 28.48},
    "Vistula Lagoon Baltiysk": {
        "name": "Vistula Lagoon Baltiysk",
        "Latitude": 54.35,
        "Longitude": 19.82,
    },
}

# Offshore patrol / training box seeds (not harbor-to-harbor legs).
PATROL_ZONE_SEEDS: List[Dict[str, float | str]] = [
    {"name": "Fehmarn Belt", "Latitude": 54.55, "Longitude": 11.35},
    {"name": "Mecklenburg Bight", "Latitude": 54.25, "Longitude": 12.45},
    {"name": "Swedish EEZ south", "Latitude": 57.05, "Longitude": 18.40},
    {"name": "Gotland training box", "Latitude": 57.35, "Longitude": 18.90},
    {"name": "Bornholm approaches", "Latitude": 55.20, "Longitude": 15.55},
    {"name": "Gulf of Finland west", "Latitude": 59.75, "Longitude": 22.10},
]


class RusFleetUnit(TypedDict):
    class_: str  # stored as "class" in dicts
    name: str
    mmsi: str
    home_base: str
    vessel_category: str
    default_mission_state: str
    data_source: str


def _u(
    cls: str,
    name: str,
    mmsi: str,
    home_base: str,
    vessel_category: str,
    default_mission_state: str,
    data_source: str = "real",
) -> Dict[str, str]:
    return {
        "class": cls,
        "name": name,
        "mmsi": mmsi,
        "home_base": home_base,
        "vessel_category": vessel_category,
        "default_mission_state": default_mission_state,
        "data_source": data_source,
    }


RUS_BALTIC_FLEET: List[Dict[str, str]] = [
    _u("Project 11540 Neustrashimy", "Yaroslav Mudry", "RUS273001", "Baltiysk", "frigate", "eligible_mission"),
    _u("Project 20380 Steregushchiy", "Steregushchy", "RUS273002", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 20380 Steregushchiy", "Soobrazitelny", "RUS273003", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 20380 Steregushchiy", "Boikiy", "RUS273004", "Baltiysk", "corvette", "home_standby"),
    _u("Project 20380 Steregushchiy", "Stoikiy", "RUS273005", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 20385 Gremyashchiy", "Gremyashchiy", "RUS273006", "Baltiysk", "corvette", "home_standby"),
    _u("Project 21631 Buyan-M", "Grad Sviyazhsk", "RUS273007", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 21631 Buyan-M", "Velikiy Ustyug", "RUS273008", "Baltiysk", "corvette", "home_standby"),
    _u("Project 21631 Buyan-M", "Serpukhov", "RUS273009", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 22800 Karakurt", "Mytishchi", "RUS273010", "Baltiysk", "corvette", "home_standby"),
    _u("Project 22800 Karakurt", "Sovetsk", "RUS273011", "Baltiysk", "corvette", "eligible_mission"),
    _u("Project 22800 Karakurt", "Odintsovo", "RUS273012", "Baltiysk", "corvette", "home_standby"),
    _u("Project 877 Kilo", "Dmitrov", "RUS273013", "Kronstadt", "submarine", "eligible_mission"),
    _u("Project 877 Kilo", "Vladikavkaz", "RUS273014", "Kronstadt", "submarine", "home_standby"),
    _u("Project 636.3 Improved Kilo", "Kolpino", "RUS273015", "Kronstadt", "submarine", "eligible_mission"),
    _u("Project 775 Ropucha", "Kaliningrad", "RUS273016", "Baltiysk", "landing ship", "home_standby"),
    _u("Project 775 Ropucha", "Korolev", "RUS273017", "Baltiysk", "landing ship", "eligible_mission"),
    _u("Project 1171 Alligator", "Minsk", "RUS273018", "Baltiysk", "landing ship", "home_standby"),
    _u("Project 1171 Alligator", "Kalinin", "RUS273019", "Baltiysk", "landing ship", "home_standby"),
    _u("Project 860 Vishnya", "Kildin", "RUS273020", "Kronstadt", "support", "home_standby"),
    _u("Project 861M Moma", "Khariton Laptev", "RUS273021", "Kronstadt", "support", "home_standby"),
    _u("Project 20180 Altay", "Elbrus", "RUS273022", "Baltiysk", "support", "home_standby"),
    _u("Project 141 Kashtan", "Pavel Khokhryakov", "RUS273023", "Primorsk", "support", "home_standby"),
    _u("Project 1265 Sonya", "BT-215", "RUS273024", "Kronstadt", "patrol", "eligible_mission"),
    _u("Project 1331M Mukha", "PK-123", "RUS273025", "Vistula Lagoon Baltiysk", "patrol", "home_standby"),
    _u("Project 20380 Steregushchiy", "RFS Sim Corvette Alpha", "RUS273026", "Baltiysk", "corvette", "eligible_mission", "simulated"),
    _u("Project 20380 Steregushchiy", "RFS Sim Corvette Beta", "RUS273027", "Baltiysk", "corvette", "home_standby", "simulated"),
    _u("Project 22800 Karakurt", "RFS Sim Karakurt Gamma", "RUS273028", "Baltiysk", "corvette", "home_standby", "simulated"),
    _u("Project 877 Kilo", "RFS Sim Kilo Delta", "RUS273029", "Kronstadt", "submarine", "home_standby", "simulated"),
    _u("Project 775 Ropucha", "RFS Sim Ropucha Epsilon", "RUS273030", "Baltiysk", "landing ship", "eligible_mission", "simulated"),
    _u("Project 1171 Alligator", "RFS Sim Alligator Zeta", "RUS273031", "Primorsk", "landing ship", "home_standby", "simulated"),
    _u("Project 1265 Sonya", "RFS Sim Minesweeper Eta", "RUS273032", "Kronstadt", "patrol", "eligible_mission", "simulated"),
    _u("Project 1331M Mukha", "RFS Sim Patrol Theta", "RUS273033", "Vistula Lagoon Baltiysk", "patrol", "home_standby", "simulated"),
    _u("Project 860 Vishnya", "RFS Sim AGI Iota", "RUS273034", "Primorsk", "support", "home_standby", "simulated"),
    _u("Project 20180 Altay", "RFS Sim Tug Kappa", "RUS273035", "Baltiysk", "support", "home_standby", "simulated"),
]

NAME_BY_MMSI: Dict[str, str] = {u["mmsi"]: u["name"] for u in RUS_BALTIC_FLEET}
UNIT_BY_MMSI: Dict[str, Dict[str, str]] = {u["mmsi"]: u for u in RUS_BALTIC_FLEET}

MISSION_ELIGIBLE_FRACTION = 0.33
BALTIYSK_HOME_FRACTION = 0.60


def fleet_units_for_simulation(num_ships: int | None = None) -> List[Dict[str, str]]:
    """Return fleet slice; default is full registry."""
    if num_ships is None or num_ships >= len(RUS_BALTIC_FLEET):
        return list(RUS_BALTIC_FLEET)
    return RUS_BALTIC_FLEET[: max(1, num_ships)]


def home_base_for_unit(unit: Dict[str, str]) -> Dict[str, Any]:
    key = unit.get("home_base", "Baltiysk")
    return dict(RUS_HOME_BASES.get(key, RUS_HOME_BASES["Baltiysk"]))
