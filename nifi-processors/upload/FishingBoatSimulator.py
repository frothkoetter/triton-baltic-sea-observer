import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import io
import json
import os
import random

from fishing_boat_mission_engine import (
    generate_fishing_ais_message,
    load_fishing_boat_state,
    save_fishing_boat_state,
    update_fishing_boat_movements,
)
from fishing_fleet_registry import FISHING_BOAT_STATE_FILE, SIMULATION_ROUTE_NAME

VERSION_STATE_FILE = "/tmp/fishing_boat_version_state.json"
DEFAULT_NUM_FISHING_BOATS = 10


class FishingBoatSimulator(FlowFileTransform):
    """German Baltic fishing fleet: home-port tours, 12–36 h, low speed."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "German fishing vessels (german_fishing_tours): depart registered home port, "
            "low-speed fishing grounds, return same port within 12–36 simulated hours. "
            f"State: {FISHING_BOAT_STATE_FILE}"
        )
        dependencies = ["numpy", "pyproj>=3.6.0"]

    NUM_FISHING_BOATS = PropertyDescriptor(
        name="Number of Fishing Boats",
        description="Concurrent German fishing boats to simulate (max 20 in fleet registry).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_FISHING_BOATS),
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_FISHING_BOATS]
        self.last_num = self._load_last_num()

    def getPropertyDescriptors(self):
        return self.descriptors

    def _load_last_num(self):
        if os.path.exists(VERSION_STATE_FILE):
            try:
                with open(VERSION_STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("num_fishing_boats")
            except json.JSONDecodeError:
                return None
        return None

    def _save_last_num(self, n: int) -> None:
        with open(VERSION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"num_fishing_boats": n}, f)

    def transform(self, context, flowFile) -> FlowFileTransformResult:
        try:
            raw = context.getProperty(self.NUM_FISHING_BOATS.name).getValue()
            num = int(raw) if raw else DEFAULT_NUM_FISHING_BOATS

            if self.last_num is None or self.last_num != num:
                self.logger.info(
                    f"Fishing boat count changed ({self.last_num} -> {num}); re-init state."
                )
                if os.path.exists(FISHING_BOAT_STATE_FILE):
                    os.remove(FISHING_BOAT_STATE_FILE)
                self.last_num = num
                self._save_last_num(num)

            rng = random.Random()
            ships = load_fishing_boat_state(num, rng=rng)
            ships = update_fishing_boat_movements(ships, rng=rng)
            save_fishing_boat_state(ships)

            output_content = io.StringIO()
            for ship in ships:
                output_content.write(json.dumps(generate_fishing_ais_message(ship)) + "\n")

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
            self.logger.error(f"Error during fishing boat simulation: {e}")
            return FlowFileTransformResult(
                contents=f"Error processing fishing boat simulation: {e}".encode("utf-8"),
                attributes={"error": str(e), "ship.simulation.error": "true"},
                relationship="failure",
            )
