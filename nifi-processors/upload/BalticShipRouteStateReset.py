"""
NiFi processor: reset AIS / marine ship routing state in /tmp JSON files only.

Does not touch LiDAR temp dirs, Iceberg, or MarineShipSimulator ``num_ships`` version file.
"""

import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

import json
from typing import Any, Dict

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from baltic_ship_sim_state import (
    AIS_SHIP_STATE_FILE,
    reset_baltic_ship_route_state,
    reset_payload,
)
from marine_fleet_registry import MARINESHIP_STATE_FILE


class BalticShipRouteStateReset(FlowFileTransform):
    """Remove or clear route fields in /tmp ship state for AIS and marine simulators."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Resets Baltic AIS (ShipSimulationRoutesGemini) and marine "
            "(MarineShipSimulator) routing state in /tmp JSON files only. "
            "Optionally writes pre-reset file contents to the outgoing FlowFile "
            "payload, then deletes or clears route fields. "
            "Use after routing or water-mask upgrades without wiping other NiFi data."
        )
        dependencies: list[str] = []

    RESET_AIS = PropertyDescriptor(
        name="Reset AIS State",
        description=f"Target {AIS_SHIP_STATE_FILE} (ShipSimulationRoutesGemini AIS simulator).",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )
    RESET_MARINE = PropertyDescriptor(
        name="Reset Marine State",
        description=f"Target {MARINESHIP_STATE_FILE} (MarineShipSimulator).",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )
    RESET_MODE = PropertyDescriptor(
        name="Reset Mode",
        description=(
            "Delete State Files: remove JSON files (full re-init on next simulation tick). "
            "Clear Route Fields Only: keep ship positions/MMSI but empty RouteWaypoints "
            "and RouteIndex so routes are recomputed on water."
        ),
        allowable_values=["Delete State Files", "Clear Route Fields Only"],
        default_value="Delete State Files",
        required=True,
    )
    DUMP_STATE = PropertyDescriptor(
        name="Dump State To Payload",
        description=(
            "When true, read each targeted /tmp JSON file before reset and include "
            "the raw content under snapshots in the FlowFile body (then delete or clear)."
        ),
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [
            self.RESET_AIS,
            self.RESET_MARINE,
            self.RESET_MODE,
            self.DUMP_STATE,
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile) -> FlowFileTransformResult:
        try:
            reset_ais = context.getProperty(self.RESET_AIS.name).getValue() == "true"
            reset_marine = context.getProperty(self.RESET_MARINE.name).getValue() == "true"
            mode_label = context.getProperty(self.RESET_MODE.name).getValue()
            mode = (
                "clear_route_fields"
                if mode_label == "Clear Route Fields Only"
                else "delete_files"
            )
            dump_state = context.getProperty(self.DUMP_STATE.name).getValue() == "true"
            if not reset_ais and not reset_marine:
                return FlowFileTransformResult(
                    contents=b'{"skipped": true, "reason": "no targets selected"}',
                    attributes={"ship.route.reset": "skipped"},
                    relationship="failure",
                )

            result = reset_baltic_ship_route_state(
                reset_ais=reset_ais,
                reset_marine=reset_marine,
                mode=mode,
            )
            body = reset_payload(result, include_snapshots=dump_state)
            summary: Dict[str, Any] = body["reset"]
            attrs = {
                "mime.type": "application/json",
                "ship.route.reset.dump_state": "true" if dump_state else "false",
            }
            if dump_state and reset_ais:
                ais_snap = result.snapshots.get(AIS_SHIP_STATE_FILE)
                if isinstance(ais_snap, list):
                    attrs["ship.state.ais.count"] = str(len(ais_snap))
                    empty_routes = sum(
                        1
                        for s in ais_snap
                        if isinstance(s, dict) and not s.get("RouteWaypoints")
                    )
                    attrs["ship.state.ais.empty_routes"] = str(empty_routes)
            if result.errors:
                summary["ok"] = False
                body["reset"] = summary
                return FlowFileTransformResult(
                    contents=json.dumps(body, indent=2).encode("utf-8"),
                    attributes={**attrs, "ship.route.reset": "error"},
                    relationship="failure",
                )

            return FlowFileTransformResult(
                contents=json.dumps(body, indent=2).encode("utf-8"),
                attributes={**attrs, "ship.route.reset": "true"},
                relationship="success",
            )
        except Exception as exc:
            self.logger.error(f"Route state reset failed: {exc}")
            return FlowFileTransformResult(
                contents=json.dumps({"error": str(exc)}).encode("utf-8"),
                attributes={"ship.route.reset": "error"},
                relationship="failure",
            )


if __name__ == "__main__":
    r = reset_baltic_ship_route_state(reset_ais=True, reset_marine=True, mode="delete_files")
    print(json.dumps(reset_payload(r), indent=2))
