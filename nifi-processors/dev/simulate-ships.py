from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import io
import os
import json
import random
from datetime import datetime, timezone

# File to persist ship state
STATE_FILE = "/tmp/ship_state.json"

def generate_ais_message(ship):
    """Create an AIS message for a ship."""
    return {
        "MMSI": ship["MMSI"],
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(ship["Latitude"], 5),
        "Longitude": round(ship["Longitude"], 5),
        "Speed": round(ship["Speed"], 1),
        "Course": round(ship["Course"], 1),
        "Status": ship["Status"]
    }

def load_ship_state(num_ships):
    """Load ship state from file or initialize."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    else:
        return initialize_ships(num_ships)

def save_ship_state(ships):
    """Save ship state to file."""
    with open(STATE_FILE, "w") as f:
        json.dump(ships, f)

def initialize_ships(num_ships):
    """Randomly initialize ship states."""
    ships = []
    for i in range(num_ships):
        ship = {
            "MMSI": 123456000 + i,
            "Latitude": 56.36 + random.uniform(-0.2, 0.2),
            "Longitude": 18.39 + random.uniform(-0.2, 0.2),
            "Speed": random.uniform(1, 10),
            "Course": random.uniform(0, 360),
            "Status": random.choice([
                "Underway using engine", "Underway", "Anchored", 
                "Moored", "Not under command"
            ])
        }
        ships.append(ship)
    return ships

def update_ship_movements(ships):
    """Simulate ship movements randomly."""
    for ship in ships:
        ship["Latitude"] += random.uniform(-0.05, 0.01)
        ship["Longitude"] += random.uniform(-0.05, 0.01)
        ship["Speed"] = max(0, ship["Speed"] + random.uniform(-1, 1))
        ship["Course"] = (ship["Course"] + random.uniform(-5, 5)) % 360
        ship["Status"] = random.choice([
            "Underway using engine", "Underway", "Anchored", 
            "Moored", "Not under command"
        ])
    return ships

class simulateShips(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.3'
        description = 'Simulate Ship movements in the Baltic Sea (JSON output with persisted state)'
        dependencies = ['geohash2']

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
            # Get number of ships
            num_ships_val = context.getProperty("Number Ships").getValue()
            num_ships = int(num_ships_val) if num_ships_val else 5

            # Load or create ship state
            ships = load_ship_state(num_ships)
            ships = update_ship_movements(ships)
            save_ship_state(ships)

            # Generate NDJSON output
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

