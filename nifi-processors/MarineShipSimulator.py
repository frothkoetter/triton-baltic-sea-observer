from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import io
import os
import json
import random
import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Union

# --- Konstanten ---
STATE_FILE = "/tmp/marineship_state.json"
# Neu: Separate Datei, um die Anzahl der Schiffe zu verfolgen
VERSION_STATE_FILE = "/tmp/marineship_version_state.json" 

DEFAULT_NUM_SHIPS = 5
PROXIMITY_THRESHOLD = 0.05
NAUTICAL_MILE_TO_DEGREE_APPROX = 0.016666666666666666
SPEED_FLUCTUATION = 0.5

OPERATIONAL_STATUS_OPTIONS = ["Fully Operational", "Limited Operational", "Non-Operational"]
SYSTEM_STATUS_OPTIONS = ["All Systems Green", "Minor Sensor Issues", "Major Engine Failure", "Weapon System Offline"]

HARBORS = [
    {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Świnoujście", "Latitude": 53.9106, "Longitude": 14.2478},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Rønne", "Latitude": 55.1037, "Longitude": 14.7065},
    {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
    {"name": "Turku", "Latitude": 60.4518, "Longitude": 22.2666},
    {"name": "Paldiski", "Latitude": 59.3567, "Longitude": 24.0539},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Ventspils", "Latitude": 57.3890, "Longitude": 21.5610},
    {"name": "Wismar", "Latitude": 53.8934, "Longitude": 11.4536},
    {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    {"name": "Gdańsk", "Latitude": 54.3520, "Longitude": 18.6466},
    {"name": "Ustka", "Latitude": 54.5801, "Longitude": 16.8596}
]

HARBOR_LOOKUP = {harbor["name"]: harbor for harbor in HARBORS}

def generate_ais_message(ship: Dict[str, Any]) -> Dict[str, Union[str, float, int]]:
    """Generates a single AIS-like message for a specific ship."""
    message = {
        "MMSI": ship["MMSI"],
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(ship["Latitude"], 5),
        "Longitude": round(ship["Longitude"], 5),
        "Speed": round(ship["Speed"], 1),
        "Course": round(ship["Course"], 1),
        "Status": ship["Status"],
        "Destination": ship["Destination"]["name"],
        "Depth": round(ship["Depth"], 1),
        "Operational_Status": ship["Operational_Status"],
        "System_Status": ship["System_Status"]
    }
    return message

def load_ship_state(num_ships: int) -> List[Dict[str, Any]]:
    """
    Loads the ship state from a file or initializes new ships if the file does not exist.
    Ensures 'Destination' is always a harbor dictionary and new fields are present.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            ships = json.load(f)
        for ship in ships:
            dest_name = ship.get("Destination")
            if isinstance(dest_name, str):
                ship["Destination"] = HARBOR_LOOKUP.get(dest_name, random.choice(HARBORS))
            elif isinstance(dest_name, dict):
                ship["Destination"] = HARBOR_LOOKUP.get(dest_name.get("name"), random.choice(HARBORS))
            else:
                ship["Destination"] = random.choice(HARBORS)

            if "Depth" not in ship:
                ship["Depth"] = 0.0
            if "Operational_Status" not in ship:
                ship["Operational_Status"] = random.choice(OPERATIONAL_STATUS_OPTIONS)
            if "System_Status" not in ship:
                ship["System_Status"] = random.choice(SYSTEM_STATUS_OPTIONS)
        return ships
    else:
        return initialize_ships(num_ships)

def save_ship_state(ships: List[Dict[str, Any]]):
    """Saves the current state of the ships to a file."""
    serializable_ships = []
    for ship in ships:
        ship_copy = ship.copy()
        if isinstance(ship_copy["Destination"], dict):
            ship_copy["Destination"] = ship_copy["Destination"]["name"]
        serializable_ships.append(ship_copy)

    with open(STATE_FILE, "w") as f:
        json.dump(serializable_ships, f)

def initialize_ships(num_ships: int) -> List[Dict[str, Any]]:
    """Initializes a new set of ships."""
    ships = []
    for i in range(num_ships):
        destination = random.choice(HARBORS)
        initial_status = random.choice(["Underway using engine", "Underway", "Anchored", "Moored", "Not under command"])
        initial_depth = 0.0 if initial_status == "Moored" else random.uniform(5.0, 15.0)
        ship = {
            "MMSI": f"MAR{123400 + i}",
            "Latitude": destination["Latitude"] + random.uniform(-1.0, 1.0),
            "Longitude": destination["Longitude"] + random.uniform(-1.0, 1.0),
            "Speed": random.uniform(1, 10),
            "Course": random.uniform(0, 360),
            "Status": initial_status,
            "Destination": destination,
            "Depth": initial_depth,
            "Operational_Status": random.choice(OPERATIONAL_STATUS_OPTIONS),
            "System_Status": random.choice(SYSTEM_STATUS_OPTIONS)
        }
        ships.append(ship)
    return ships

def move_towards(lat1: float, lon1: float, lat2: float, lon2: float, speed: float) -> (float, float):
    """Calculates the next position of a ship moving towards a destination."""
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    dist_deg = math.hypot(delta_lat, delta_lon)

    if dist_deg == 0:
        return lat2, lon2

    move_deg = speed * NAUTICAL_MILE_TO_DEGREE_APPROX
    ratio = min(1.0, move_deg / dist_deg)

    return lat1 + delta_lat * ratio, lon1 + delta_lon * ratio

def update_ship_movements(ships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Updates the position, course, speed, and status of each ship."""
    for ship in ships:
        dest = ship["Destination"]

        new_lat, new_lon = move_towards(
            ship["Latitude"], ship["Longitude"],
            dest["Latitude"], dest["Longitude"],
            ship["Speed"]
        )
        ship["Latitude"] = new_lat
        ship["Longitude"] = new_lon

        if abs(dest["Latitude"] - new_lat) > 1e-6 or abs(dest["Longitude"] - new_lon) > 1e-6:
            ship["Course"] = (math.degrees(math.atan2(dest["Longitude"] - new_lon, dest["Latitude"] - new_lat)) + 360) % 360
        else:
            ship["Course"] = ship["Course"]

        ship["Speed"] = max(1.0, ship["Speed"] + random.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION))

        if ship["Status"] != "Moored":
            ship["Status"] = random.choice(["Underway using engine", "Underway"])
            ship["Depth"] = random.uniform(5.0, 15.0)
        else:
            ship["Depth"] = 0.0

        if abs(ship["Latitude"] - dest["Latitude"]) < PROXIMITY_THRESHOLD and \
           abs(ship["Longitude"] - dest["Longitude"]) < PROXIMITY_THRESHOLD:
            new_dest = random.choice([h for h in HARBORS if h["name"] != dest["name"]])
            ship["Destination"] = new_dest
            ship["Status"] = "Moored"
            ship["Speed"] = 0.0
            ship["Depth"] = 0.0

    return ships

class MarineShipSimulator(FlowFileTransform):
    """NiFi Python Processor for simulating ship movements."""
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.3.0'
        description = 'Simulates ship movements between Baltic Sea harbors using realistic routes and persistent state, including extended J2.2 status fields.'
        dependencies = []

    NUM_SHIPS = PropertyDescriptor(
        name="Number of Ships",
        description="Anzahl der zu simulierenden Schiffe",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_SHIPS),
        required=True
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS]
        # Neu: Speichere den letzten num_ships Wert
        self.last_num_ships = self.get_last_num_ships()

    def getPropertyDescriptors(self):
        return self.descriptors

    def get_last_num_ships(self):
        """Lädt den letzten num_ships-Wert aus der Versionsdatei."""
        if os.path.exists(VERSION_STATE_FILE):
            with open(VERSION_STATE_FILE, "r") as f:
                try:
                    state = json.load(f)
                    return state.get("num_ships")
                except json.JSONDecodeError:
                    return None
        return None

    def save_last_num_ships(self, num_ships):
        """Speichert den aktuellen num_ships-Wert in die Versionsdatei."""
        with open(VERSION_STATE_FILE, "w") as f:
            json.dump({"num_ships": num_ships}, f)

    def transform(self, context, flowFile) -> FlowFileTransformResult:
        """
        Transforms the incoming FlowFile by generating ship simulation data.
        """
        try:
            num_ships_val = context.getProperty(self.NUM_SHIPS.name).getValue()
            num_ships = int(num_ships_val) if num_ships_val else DEFAULT_NUM_SHIPS
            
            # Neue Logik: Prüfe, ob sich die Anzahl der Schiffe geändert hat
            if self.last_num_ships is None or self.last_num_ships != num_ships:
                self.logger.info(f"Number of ships changed from {self.last_num_ships} to {num_ships}. Reinitializing simulation.")
                
                # Lösche die alte Zustandsdatei, um eine Neuinitialisierung zu erzwingen
                if os.path.exists(STATE_FILE):
                    os.remove(STATE_FILE)
                
                # Aktualisiere den Versionszustand
                self.last_num_ships = num_ships
                self.save_last_num_ships(num_ships)

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
            self.logger.error(f"Error during ship simulation: {e}")
            return FlowFileTransformResult(
                contents=f"Error processing ship simulation: {e}".encode("utf-8"),
                attributes={"error": str(e), "ship.simulation.error": "true"},
                relationship="failure"
            )
