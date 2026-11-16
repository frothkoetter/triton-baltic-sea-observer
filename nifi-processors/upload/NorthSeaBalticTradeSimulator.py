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

from north_sea_baltic_fleet_registry import (
    MAX_FLEET_SLOTS,
    NSB_TRADE_STATE_FILE,
    SIMULATION_ROUTE_NAME,
)
from north_sea_baltic_transit_engine import (
    generate_nsb_ais_message,
    load_nsb_trade_state,
    save_nsb_trade_state,
    update_nsb_trade_movements,
    warm_nsb_pair_routes,
)

VERSION_STATE_FILE = "/tmp/nsb_trade_version_state.json"
DEFAULT_NUM_SHIPS = 1000


class NorthSeaBalticTradeSimulator(FlowFileTransform):
    """North Sea ↔ Baltic trade AIS (freight, tanker, container, gas)."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Trade vessels (north_sea_baltic_trade): load/unload on North Sea hubs, "
            "transit to Baltic ports and return. 1 NiFi minute = 1 simulated hour; "
            "one AIS record per ship per tick. Deterministic fleet MMSI 311040001+."
        )
        dependencies = ["numpy", "pyproj>=3.6.0"]

    NUM_SHIPS = PropertyDescriptor(
        name="Number of Ships",
        description=f"Concurrent trade vessels (500–{MAX_FLEET_SLOTS}, default 1000).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_NUM_SHIPS),
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_SHIPS]
        self.last_num = self._load_last_num()

    def getPropertyDescriptors(self):
        return self.descriptors

    def _load_last_num(self):
        if os.path.exists(VERSION_STATE_FILE):
            try:
                with open(VERSION_STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("num_ships")
            except json.JSONDecodeError:
                return None
        return None

    def _save_last_num(self, n: int) -> None:
        with open(VERSION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"num_ships": n}, f)

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        try:
            raw = context.getProperty(self.NUM_SHIPS.name).getValue()
            num = int(raw) if raw else DEFAULT_NUM_SHIPS
            num = min(max(1, num), MAX_FLEET_SLOTS)

            if self.last_num is None or self.last_num != num:
                self.logger.info(
                    f"NSB trade ship count changed ({self.last_num} -> {num}); re-init state."
                )
                if os.path.exists(NSB_TRADE_STATE_FILE):
                    os.remove(NSB_TRADE_STATE_FILE)
                self._save_last_num(num)
                self.last_num = num

            warm_nsb_pair_routes()
            ships = load_nsb_trade_state(num)
            ships = update_nsb_trade_movements(ships)
            save_nsb_trade_state(ships)

            buf = io.StringIO()
            for ship in ships:
                buf.write(json.dumps(generate_nsb_ais_message(ship)) + "\n")

            return FlowFileTransformResult(
                contents=buf.getvalue().encode("utf-8"),
                attributes={
                    "format": "ndjson",
                    "ship.simulation": "true",
                    "ship.simulation.route": SIMULATION_ROUTE_NAME,
                    "ship.count": str(len(ships)),
                },
                relationship="success",
            )
        except Exception as exc:
            self.logger.error(f"NSB trade simulation failed: {exc}")
            return FlowFileTransformResult(
                contents=json.dumps({"error": str(exc)}).encode("utf-8"),
                attributes={"ship.simulation.error": "true"},
                relationship="failure",
            )