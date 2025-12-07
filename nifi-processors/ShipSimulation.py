from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import io
import os
import json
import random
import math
from datetime import datetime, timezone

STATE_FILE = "/tmp/ship_state.json"

# Baltic Sea harbors
HARBORS = [
    {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
  { "name": "Turku", "Latitude": 60.4518, "Longitude": 22.2666 },       // Finland
  { "name": "Mariehamn", "Latitude": 60.0973, "Longitude": 19.9348 },   // Åland Islands
  { "name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136 },     // Latvia
  { "name": "Ventspils", "Latitude": 57.3890, "Longitude": 21.5610 },   // Latvia
  { "name": "Kaliningrad", "Latitude": 54.7104, "Longitude": 20.4522 }, // Russia
  { "name": "Świnoujście", "Latitude": 53.9106, "Longitude": 14.2478 }, // Poland
  { "name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405 },     // Germany
  { "name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672 },  // Germany
  { "name": "St. Petersburg", "Latitude": 59.9343, "Longitude": 30.3351 }, // Russia
  { "name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869 }   // Sweden
]

def generate_ais_message(ship):
    return {
        "MMSI": ship["MMSI"],
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(ship["Latitude"], 5),
        "Longitude": round(ship["Longitude"], 5),
        "Speed": round(ship["Speed"], 1),
        "Course": round(ship["Course"], 1),
        "Status": ship["Status"],
        "Destination": ship["Destination"]["name"]
    }

def load_ship_state(num_ships):
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    else:
        return initialize_ships(num_ships)

def save_ship_state(ships):
    with open(STATE_FILE, "w") as f:
        json.dump(ships, f)

def initialize_ships(num_ships):
    ships = []
    for i in range(num_ships):
        start = random.choice(HARBORS)
        dest = random.choice([h for h in HARBORS if h != start])
        ships.append({
            "MMSI": 123456000 + i,
            "Latitude": start["Latitude"],
            "Longitude": start["Longitude"],
            "Speed": random.uniform(5, 15),
            "Course": 180.0,
            "Status": "Underway using engine",
            "Destination": dest
        })
    return ships

def move_towards(lat1, lon1, lat2, lon2, speed):
    """ Move toward destination (approx. 1 NM ≈ 1/60 deg) """
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    dist_deg = math.hypot(delta_lat, delta_lon)
    if dist_deg == 0:
        return lat2, lon2
    move_deg = (speed / 60.0) * 0.1  # move per tick
    ratio = move_deg / dist_deg
    return lat1 + delta_lat * ratio, lon1 + delta_lon * ratio

def update_ship_movements(ships):
    for ship in ships:
        dest = ship["Destination"]
        new_lat, new_lon = move_towards(
            ship["Latitude"], ship["Longitude"],
            dest["Latitude"], dest["Longitude"],
            ship["Speed"]
        )
        ship["Latitude"] = new_lat
        ship["Longitude"] = new_lon
        ship["Course"] = (math.degrees(math.atan2(dest["Longitude"] - new_lon, dest["Latitude"] - new_lat)) + 360) % 360
        ship["Speed"] = max(1.0, ship["Speed"] + random.uniform(-0.5, 0.5))
        ship["Status"] = random.choice([
            "Underway using engine", "Underway", "Moored"
        ])

        # Check if arrived
        if abs(ship["Latitude"] - dest["Latitude"]) < 0.05 and abs(ship["Longitude"] - dest["Longitude"]) < 0.05:
            new_dest = random.choice([h for h in HARBORS if h["name"] != dest["name"]])
            ship["Destination"] = new_dest
            ship["Status"] = "Moored"
            ship["Speed"] = 0.0

    return ships

class ShipsSimulationRoute(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.4'
        description = 'Simulate realistic ship movements in the Baltic Sea'
        dependencies = []

    NUM_SHIPS = PropertyDescriptor(
        name="Number Ships",
        description="Number of ships to simulate",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="5",
        required=False
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        try:
            num_ships_val = context.getProperty("Number Ships").getValue()
            num_ships = int(num_ships_val) if num_ships_val else 5

            ships = load_ship_state(num_ships)
            ships = update_ship_movements(ships)
            save_ship_state(ships)

            output = io.StringIO()
            for ship in ships:
                msg = generate_ais_message(ship)
                output.write(json.dumps(msg) + "\n")

            return FlowFileTransformResult(
                contents=output.getvalue().encode("utf-8"),
                attributes={"format": "ndjson", "ship.simulation": "true"},
                relationship="success"
            )
        except Exception as e:
            return FlowFileTransformResult(
                contents=str(e).encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure"
            )

