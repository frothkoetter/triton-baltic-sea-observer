import sys
from pathlib import Path

# NiFi loads this file from python/extensions but runs the processor in an isolated
# venv; sibling modules are only importable if the extensions dir is on sys.path.
_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import io
import os
import json
import random
import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Union, Tuple

from baltic_ship_routing import (
    advance_route_or_arrive,
    ensure_ship_route,
    move_towards_on_water,
    route_target,
)
from baltic_water_placement import (
    is_baltic_water,
    sample_water_near_harbor,
    snap_to_baltic_water,
)

from baltic_ship_sim_state import AIS_SHIP_STATE_FILE

# --- Constants ---
STATE_FILE = AIS_SHIP_STATE_FILE
DEFAULT_NUM_SHIPS = 5
PROXIMITY_THRESHOLD = 0.05  # Degrees for harbor arrival detection
NAUTICAL_MILE_TO_DEGREE_APPROX = 0.016666666666666666 # Approx 1 degree = 60 nautical miles
SPEED_FLUCTUATION = 0.5 # Max random speed change

HARBORS = [
    {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
    {"name": "Turku", "Latitude": 60.4518, "Longitude": 22.2666},
    {"name": "Mariehamn", "Latitude": 60.0973, "Longitude": 19.9348},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Ventspils", "Latitude": 57.395, "Longitude": 21.520},
    {"name": "Kaliningrad", "Latitude": 54.7104, "Longitude": 20.4522},
    {"name": "Świnoujście", "Latitude": 53.9106, "Longitude": 14.2478},
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672},
    {"name": "St. Petersburg", "Latitude": 59.904, "Longitude": 29.773},
    {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
    {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    {"name": "Wismar", "Latitude": 53.8934, "Longitude": 11.4536},
    {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    {"name": "Greifswald", "Latitude": 54.0934, "Longitude": 13.3781},
    {"name": "Nynäshamn", "Latitude": 58.9036, "Longitude": 17.9470},
    {"name": "Ustka", "Latitude": 54.5801, "Longitude": 16.8596},
    {"name": "Pori", "Latitude": 61.4847, "Longitude": 21.7976},
    {"name": "Gdańsk", "Latitude": 54.3520, "Longitude": 18.6466},
    {"name": "Paldiski", "Latitude": 59.3567, "Longitude": 24.0539},
    {"name": "Rønne", "Latitude": 55.1037, "Longitude": 14.7065},
    {"name": "Visby", "Latitude": 57.6409, "Longitude": 18.2960},
    {"name": "Bolderāja", "Latitude": 56.9950, "Longitude": 24.0500},
    {"name": "Primorsk", "Latitude": 60.338, "Longitude": 28.480}
]

# Create a dictionary for quick harbor lookup by name
HARBOR_LOOKUP = {harbor["name"]: harbor for harbor in HARBORS}

def generate_ais_message(ship: Dict[str, Any]) -> Dict[str, Union[str, float, int]]:
    """Generates a single AIS message for a given ship."""
    return {
        "MMSI": ship["MMSI"],
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(ship["Latitude"], 5),
        "Longitude": round(ship["Longitude"], 5),
        "Speed": round(ship["Speed"], 1),
        "Course": round(ship["Course"], 1),
        "Status": ship["Status"],
        "Destination": ship["Destination"]["name"],
        "OriginPort": (
            ship["OriginPort"]["name"]
            if isinstance(ship.get("OriginPort"), dict)
            else ship.get("OriginPort")
        ),
    }

def load_ship_state(num_ships: int) -> List[Dict[str, Any]]:
    """
    Loads ship states from a file or initializes new ships if the file doesn't exist.
    Ensures 'Destination' is always a harbor dictionary.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            ships = json.load(f)
            for ship in ships:
                # Ensure Destination is a dict, not just a name string
                dest_name = ship.get("Destination")
                if isinstance(dest_name, str):
                    ship["Destination"] = HARBOR_LOOKUP.get(dest_name, random.choice(HARBORS))
                elif isinstance(dest_name, dict):
                    # If it's already a dict, ensure it's a valid harbor from our list
                    ship["Destination"] = HARBOR_LOOKUP.get(dest_name.get("name"), random.choice(HARBORS))
                else:
                    ship["Destination"] = random.choice(HARBORS)
                origin_name = ship.get("OriginPort")
                if isinstance(origin_name, str):
                    ship["OriginPort"] = HARBOR_LOOKUP.get(origin_name, ship["Destination"])
                elif not isinstance(ship.get("OriginPort"), dict):
                    ship["OriginPort"] = ship["Destination"]
                lon0, lat0 = float(ship["Longitude"]), float(ship["Latitude"])
                if not is_baltic_water(lon0, lat0):
                    lon, lat = snap_to_baltic_water(lon0, lat0)
                    ship["Longitude"], ship["Latitude"] = lon, lat
                    ship["RouteWaypoints"] = []
                    ship["RouteIndex"] = 0
                ensure_ship_route(ship)
            return ships
    else:
        return initialize_ships(num_ships)

def save_ship_state(ships: List[Dict[str, Any]]):
    """Saves the current state of ships to a file."""
    serializable_ships = []
    for ship in ships:
        ship_copy = ship.copy()
        # Store only the destination name for serialization
        if isinstance(ship_copy["Destination"], dict):
            ship_copy["Destination"] = ship_copy["Destination"]["name"]
        if isinstance(ship_copy.get("OriginPort"), dict):
            ship_copy["OriginPort"] = ship_copy["OriginPort"]["name"]
        serializable_ships.append(ship_copy)
    with open(STATE_FILE, "w") as f:
        json.dump(serializable_ships, f)

def initialize_ships(num_ships: int) -> List[Dict[str, Any]]:
    """Initializes a new set of ships with random starting positions and destinations."""
    ships = []
    for i in range(num_ships):
        origin = random.choice(HARBORS)
        others = [h for h in HARBORS if h["name"] != origin["name"]]
        destination = random.choice(others)
        lon, lat = sample_water_near_harbor(
            origin["Longitude"],
            origin["Latitude"],
            min_km=5.0,
            max_km=25.0,
        )
        ship = {
            "MMSI": 123456000 + i,
            "Latitude": lat,
            "Longitude": lon,
            "Speed": random.uniform(1, 10),
            "Course": random.uniform(0, 360),
            "Status": random.choice([
                "Underway using engine", "Underway", "Anchored",
                "Moored", "Not under command"
            ]),
            "OriginPort": origin,
            "Destination": destination,
            "RouteWaypoints": [],
            "RouteIndex": 0,
        }
        ensure_ship_route(ship)
        ships.append(ship)
    return ships

def update_ship_movements(ships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Updates the position, course, speed, and status of each ship."""
    for ship in ships:
        lon0, lat0 = float(ship["Longitude"]), float(ship["Latitude"])
        if not is_baltic_water(lon0, lat0):
            lon, lat = snap_to_baltic_water(lon0, lat0)
            ship["Longitude"], ship["Latitude"] = lon, lat
            ship["RouteWaypoints"] = []
            ship["RouteIndex"] = 0
        target = route_target(ship)
        if target is None:
            ensure_ship_route(ship)
            target = route_target(ship)
        if target is None:
            continue
        target_lat, target_lon = target

        new_lat, new_lon = move_towards_on_water(
            ship["Latitude"],
            ship["Longitude"],
            target_lat,
            target_lon,
            ship["Speed"],
        )
        if is_baltic_water(new_lon, new_lat):
            ship["Latitude"] = new_lat
            ship["Longitude"] = new_lon
        lat, lon = ship["Latitude"], ship["Longitude"]

        if abs(target_lat - lat) > 1e-6 or abs(target_lon - lon) > 1e-6:
            ship["Course"] = (
                math.degrees(math.atan2(target_lon - lon, target_lat - lat)) + 360
            ) % 360

        ship["Speed"] = max(1.0, ship["Speed"] + random.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION))

        if ship["Status"] != "Moored":
            ship["Status"] = random.choice(["Underway using engine", "Underway"])

        advance_route_or_arrive(ship, HARBORS, lat, lon)
    return ships

class ShipSimulationProcessor(FlowFileTransform):
    """
    Nifi Python processor for simulating ship movements and generating AIS-like messages.
    Maintains persistent ship state across invocations.
    """
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.3.0'
        description = (
            'Baltic ship AIS simulation with water-only multi-waypoint routes between harbors.'
        )
        dependencies = ['numpy', 'pyproj>=3.6.0']

    NUM_SHIPS = PropertyDescriptor(
        name="Number of Ships",
        description="Number of ships to simulate",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_SHIPS), # Use constant for default
        required=True
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None) # Remove jvm if present, as it's not needed for base class init
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS]

    def getPropertyDescriptors(self) -> List[PropertyDescriptor]:
        """Returns the list of property descriptors for this processor."""
        return self.descriptors

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        """
        Transforms the incoming FlowFile by generating ship simulation data.
        """
        try:
            num_ships_val = context.getProperty(self.NUM_SHIPS.name).getValue()
            num_ships = int(num_ships_val) if num_ships_val else DEFAULT_NUM_SHIPS

            ships = load_ship_state(num_ships)
            ships = update_ship_movements(ships)
            save_ship_state(ships)

            output_content = io.StringIO()
            for ship in ships:
                msg = generate_ais_message(ship)
                output_content.write(json.dumps(msg) + "\n")

            return FlowFileTransformResult(
                contents=output_content.getvalue().encode("utf-8"),
                attributes={"format": "ndjson", "ship.simulation": "true"},
                relationship="success"
            )
        except Exception as e:
            self.logger.error(f"Error during ship simulation: {e}") # Log the error
            return FlowFileTransformResult(
                contents=f"Error processing ship simulation: {e}".encode("utf-8"),
                attributes={"error": str(e), "ship.simulation.error": "true"},
                relationship="failure"
            )
