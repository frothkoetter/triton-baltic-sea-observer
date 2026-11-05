import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import io
import os
import json
import random
from datetime import datetime, timezone
from typing import List, Dict, Any, Union

from german_navy_mission_engine import (
    SIMULATION_ROUTE_NAME,
    load_german_navy_state,
    save_german_navy_state,
    update_german_navy_movements,
)
from marine_fleet_registry import MARINESHIP_STATE_FILE, NAME_BY_MMSI

STATE_FILE = MARINESHIP_STATE_FILE
VERSION_STATE_FILE = "/tmp/marineship_version_state.json"
DEFAULT_NUM_SHIPS = 5

OPERATIONAL_STATUS_OPTIONS = ["Fully Operational", "Limited Operational", "Non-Operational"]
SYSTEM_STATUS_OPTIONS = ["All Systems Green", "Minor Sensor Issues", "Major Engine Failure", "Weapon System Offline"]


def generate_ais_message(ship: Dict[str, Any]) -> Dict[str, Union[str, float, int]]:
    dest = ship.get("Destination") or {}
    dest_name = dest.get("name") if isinstance(dest, dict) else str(dest)
    origin = ship.get("OriginPort") or ship.get("HomeBaseName") or dest_name
    if isinstance(origin, dict):
        origin = origin.get("name", dest_name)
    return {
        "MMSI": ship["MMSI"],
        "message_from": ship.get("Name") or NAME_BY_MMSI.get(ship["MMSI"], ship["MMSI"]),
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(float(ship["Latitude"]), 5),
        "Longitude": round(float(ship["Longitude"]), 5),
        "Speed": round(float(ship.get("Speed") or 0), 1),
        "Course": round(float(ship.get("Course") or 0), 1),
        "Status": ship.get("Status", "Unknown"),
        "Destination": dest_name,
        "OriginPort": origin,
        "HomeBase": ship.get("HomeBaseName", origin),
        "Phase": ship.get("Phase", ""),
        "Depth": round(float(ship.get("Depth") or 0), 1),
        "Operational_Status": ship.get("Operational_Status", "Fully Operational"),
        "System_Status": ship.get("System_Status", "All Systems Green"),
    }


class MarineShipSimulator(FlowFileTransform):
    """German Navy units: home-base missions, Baltic patrol, return, 7-day port stay."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.7.0"
        description = (
            "German Navy (german_navy_fleet): depart naval bases (Kiel, Wilhelmshaven, "
            "Eckernförde, Warnemünde), patrol Baltic / German coast 1–4 weeks, "
            "return and moore ~7 days. Water-only routing."
        )
        dependencies = ["numpy", "pyproj>=3.6.0"]

    NUM_SHIPS = PropertyDescriptor(
        name="Number of Ships",
        description="Anzahl der zu simulierenden Schiffe",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_SHIPS),
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS]
        self.last_num_ships = self.get_last_num_ships()

    def getPropertyDescriptors(self):
        return self.descriptors

    def get_last_num_ships(self):
        if os.path.exists(VERSION_STATE_FILE):
            with open(VERSION_STATE_FILE, "r") as f:
                try:
                    return json.load(f).get("num_ships")
                except json.JSONDecodeError:
                    return None
        return None

    def save_last_num_ships(self, num_ships):
        with open(VERSION_STATE_FILE, "w") as f:
            json.dump({"num_ships": num_ships}, f)

    def transform(self, context, flowFile) -> FlowFileTransformResult:
        try:
            num_ships_val = context.getProperty(self.NUM_SHIPS.name).getValue()
            num_ships = int(num_ships_val) if num_ships_val else DEFAULT_NUM_SHIPS

            if self.last_num_ships is None or self.last_num_ships != num_ships:
                self.logger.info(
                    f"Marine ship count changed ({self.last_num_ships} -> {num_ships}); re-init."
                )
                if os.path.exists(STATE_FILE):
                    os.remove(STATE_FILE)
                self.last_num_ships = num_ships
                self.save_last_num_ships(num_ships)

            ships = load_german_navy_state(
                num_ships,
                operational_status_options=OPERATIONAL_STATUS_OPTIONS,
                system_status_options=SYSTEM_STATUS_OPTIONS,
            )
            ships = update_german_navy_movements(ships, rng=random.Random())
            save_german_navy_state(ships)

            output_content = io.StringIO()
            for ship in ships:
                output_content.write(json.dumps(generate_ais_message(ship)) + "\n")

            return FlowFileTransformResult(
                contents=output_content.getvalue().encode("utf-8"),
                attributes={
                    "format": "ndjson",
                    "ship.simulation": "true",
                    "ship.simulation.route": SIMULATION_ROUTE_NAME,
                },
                relationship="success",
            )
        except Exception as e:
            self.logger.error(f"Error during marine ship simulation: {e}")
            return FlowFileTransformResult(
                contents=f"Error processing ship simulation: {e}".encode("utf-8"),
                attributes={"error": str(e), "ship.simulation.error": "true"},
                relationship="failure",
            )
