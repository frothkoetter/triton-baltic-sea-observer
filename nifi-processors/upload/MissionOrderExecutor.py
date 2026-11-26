"""
NiFi: poll or accept NBA mission orders; maintain parallel mission runtime state.

Emits JSON (one object per active mission) for RouteOnAttribute → Buoy / Marine / STANAG.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from mission_parallel import (
    DEFAULT_MAX_ACTIVE_MISSIONS,
    activate_mission_from_order,
    load_runtime_store,
    orders_to_flowfile_attributes,
    save_runtime_store,
    tick_runtime_elapsed,
)


class MissionOrderExecutor(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.1.0"
        description = (
            "Activates QUEUED NBA missions in parallel (caps via Max Active Missions). "
            "Persists bindings in triton_mission_runtime.json; emits one JSON bundle per active mission."
        )
        dependencies = []

    MAX_ACTIVE_MISSIONS = PropertyDescriptor(
        name="Max Active Missions",
        description="Parallel NBA missions on this NiFi node (resource cap).",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=str(DEFAULT_MAX_ACTIVE_MISSIONS),
        required=True,
    )

    TICK_MINUTES = PropertyDescriptor(
        name="Tick Minutes",
        description="Elapsed time added per invocation for track_duration caps.",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value="5",
        required=True,
    )

    PROCESS_QUEUED = PropertyDescriptor(
        name="Process Queued Orders",
        description="If true, FlowFile content is JSON array of orders with status QUEUED to activate.",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.MAX_ACTIVE_MISSIONS, self.TICK_MINUTES, self.PROCESS_QUEUED]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        store = load_runtime_store()
        tick_min = float(context.getProperty(self.TICK_MINUTES))
        tick_runtime_elapsed(store, tick_minutes=tick_min)

        if context.getProperty(self.PROCESS_QUEUED) == "true":
            raw = flowfile.getContentsAsBytes()
            if raw:
                try:
                    body = json.loads(raw.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    body = []
                orders = body if isinstance(body, list) else [body]
                for order in orders:
                    if str(order.get("status", "")).upper() != "QUEUED":
                        continue
                    activate_mission_from_order(store, order)

        save_runtime_store(store)
        active = [
            orders_to_flowfile_attributes(rec)
            for rec in store.active_missions()
        ]
        payload = json.dumps({"missions": active, "count": len(active)}, ensure_ascii=False)
        attrs = {
            "mission.parallel.count": str(len(active)),
            "mime.type": "application/json",
        }
        if len(active) == 1:
            attrs.update(active[0])
        return FlowFileTransformResult(
            attributes=attrs,
            contents=payload.encode("utf-8"),
        )