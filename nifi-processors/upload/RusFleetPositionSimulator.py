import sys
from pathlib import Path

_UPLOAD_DIR = Path(__file__).resolve().parent
_EXTENSIONS_DIR = _UPLOAD_DIR.parent
for _p in (_UPLOAD_DIR.as_posix(), _EXTENSIONS_DIR.as_posix()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

import io
import json
import os
import random

from rus_fleet_registry import (
    RUS_BALTIC_FLEET,
    RUS_FLEET_STATE_FILE,
    SIMULATION_ROUTE_NAME,
)
from rus_fleet_sim_engine import (
    generate_rus_ais_message,
    load_rus_fleet_state,
    save_rus_fleet_state,
    update_rus_fleet_movements,
)

DEFAULT_NUM_SHIPS = len(RUS_BALTIC_FLEET)
VERSION_STATE_FILE = "/tmp/rus_fleet_version_state.json"


class RusFleetPositionSimulator(FlowFileTransform):
    """Simulate Russian Baltic Fleet units on patrol missions and home bases."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Russian Baltic Fleet AIS-style positions (route rus_fleet_positions). "
            "Missions patrol Baltic operational boxes; ~33% active at init; "
            "7-day port stay after missions; separate state at /tmp/rus_fleet_state.json."
        )
        dependencies = ["numpy", "pyproj>=3.6.0"]

    NUM_SHIPS = PropertyDescriptor(
        name="Number of Ships",
        description="Ships to simulate (default: full rus_fleet registry).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_SHIPS),
        required=True,
    )

    RANDOM_SEED = PropertyDescriptor(
        name="Random Seed",
        description="Optional seed for reproducible mission assignment (empty = nondeterministic).",
        required=False,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS, self.RANDOM_SEED]
        self.last_num_ships = self._load_last_num_ships()

    def getPropertyDescriptors(self):
        return self.descriptors

    def _load_last_num_ships(self):
        if os.path.exists(VERSION_STATE_FILE):
            try:
                with open(VERSION_STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("num_ships")
            except json.JSONDecodeError:
                return None
        return None

    def _save_last_num_ships(self, num_ships: int) -> None:
        with open(VERSION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"num_ships": num_ships}, f)

    def transform(self, context, flowFile) -> FlowFileTransformResult:
        try:
            num_ships_val = context.getProperty(self.NUM_SHIPS.name).getValue()
            num_ships = int(num_ships_val) if num_ships_val else DEFAULT_NUM_SHIPS
            seed_val = context.getProperty(self.RANDOM_SEED.name).getValue()
            rng = random.Random(int(seed_val)) if seed_val else random.Random()

            if self.last_num_ships is None or self.last_num_ships != num_ships:
                self.logger.info(
                    f"rus_fleet ship count changed ({self.last_num_ships} -> {num_ships}); re-init state."
                )
                if os.path.exists(RUS_FLEET_STATE_FILE):
                    os.remove(RUS_FLEET_STATE_FILE)
                self.last_num_ships = num_ships
                self._save_last_num_ships(num_ships)

            ships = load_rus_fleet_state(num_ships, rng=rng)
            ships = update_rus_fleet_movements(ships, rng=rng)
            save_rus_fleet_state(ships)

            output_content = io.StringIO()
            for ship in ships:
                msg = generate_rus_ais_message(ship)
                output_content.write(json.dumps(msg) + "\n")

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
            self.logger.error(f"rus_fleet simulation error: {e}")
            return FlowFileTransformResult(
                contents=f"Error processing rus_fleet simulation: {e}".encode("utf-8"),
                attributes={"error": str(e), "ship.simulation.error": "true"},
                relationship="failure",
            )
