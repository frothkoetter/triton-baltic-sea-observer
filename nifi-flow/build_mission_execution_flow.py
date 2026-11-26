#!/usr/bin/env python3
"""Build Mission_Execution NiFi 2 process group JSON for Cloudera Data Flow import."""
from __future__ import annotations

import json
import uuid
from typing import Any

PY_BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "python-extensions",
    "version": "1.1.0",
}
STD_BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "nifi-standard-nar",
    "version": "2.6.0.4.12.0.2-1",
}
ATTR_BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "nifi-update-attribute-nar",
    "version": "2.6.0.4.12.0.2-1",
}


def _id() -> str:
    return str(uuid.uuid4())


def _proc(
    name: str,
    ptype: str,
    bundle: dict,
    x: float,
    y: float,
    props: dict | None = None,
    *,
    py: bool = False,
    schedule: str = "0 sec",
    strategy: str = "TIMER_DRIVEN",
    auto_term: list[str] | None = None,
) -> dict[str, Any]:
    pid = _id()
    return {
        "identifier": pid,
        "instanceIdentifier": None,
        "name": name,
        "comments": "",
        "position": {"x": x, "y": y},
        "type": ptype if py else ptype,
        "bundle": bundle,
        "properties": props or {},
        "propertyDescriptors": {},
        "style": {},
        "annotationData": None,
        "schedulingPeriod": schedule,
        "schedulingStrategy": strategy,
        "executionNode": "ALL",
        "penaltyDuration": "30 sec",
        "yieldDuration": "1 sec",
        "bulletinLevel": "WARN",
        "runDurationMillis": 0,
        "concurrentlySchedulableTaskCount": 1,
        "autoTerminatedRelationships": auto_term or [],
        "scheduledState": "ENABLED",
        "retryCount": 0,
        "retriedRelationships": [],
        "backoffMechanism": "PENALIZE_FLOWFILE",
        "maxBackoffPeriod": "10 mins",
        "componentType": "PROCESSOR",
        "groupIdentifier": None,  # filled by caller
    }


def _conn(
    pg_id: str,
    src: dict,
    dst: dict,
    rels: list[str],
    bends: list | None = None,
) -> dict[str, Any]:
    return {
        "identifier": _id(),
        "instanceIdentifier": None,
        "name": "",
        "comments": None,
        "position": None,
        "source": {
            "id": src["identifier"],
            "type": "PROCESSOR",
            "groupId": pg_id,
            "name": src["name"],
            "comments": None,
            "instanceIdentifier": None,
        },
        "destination": {
            "id": dst["identifier"],
            "type": "PROCESSOR",
            "groupId": pg_id,
            "name": dst["name"],
            "comments": None,
            "instanceIdentifier": None,
        },
        "labelIndex": 0,
        "zIndex": 0,
        "selectedRelationships": rels,
        "backPressureObjectThreshold": 10000,
        "backPressureDataSizeThreshold": "1 GB",
        "flowFileExpiration": "0 seconds",
        "prioritizers": [],
        "bends": bends or [],
        "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        "partitioningAttribute": None,
        "loadBalanceCompression": "DO_NOT_COMPRESS",
        "componentType": "CONNECTION",
        "groupIdentifier": pg_id,
    }


def build_mission_execution_pg() -> dict[str, Any]:
    pg_id = _id()
    demo_orders = [
        {
            "mission_id": "00000000-1111-2222-3333-444444444441",
            "flow_uuid": "demo-mission-flow-a",
            "status": "QUEUED",
            "approved_coa_id": "A",
            "target_mode": "STATIC_POINT",
            "target_latitude": 54.55,
            "target_longitude": 13.45,
            "parameters_json": {
                "surface": {"dispatch_units": 1, "unit_class": "Korvetten", "speed_cap_kn": 22},
                "buoys": {
                    "formation": "LEADING_SEMICIRCLE",
                    "count": 6,
                    "sensor_profile": "MULTIDOMAIN_MAD",
                },
                "formations": {
                    "LEADING_SEMICIRCLE": {
                        "arc_degrees": 120,
                        "offset_bearing": "ahead",
                        "standoff_km_min": 8,
                        "standoff_km_max": 14,
                    }
                },
            },
        },
        {
            "mission_id": "00000000-1111-2222-3333-444444444444",
            "flow_uuid": "demo-mission-flow-b",
            "status": "QUEUED",
            "approved_coa_id": "D",
            "target_mode": "STATIC_POINT",
            "target_latitude": 55.1,
            "target_longitude": 15.2,
            "parameters_json": {
                "surface": {"dispatch_units": 0},
                "buoys": {
                    "formation": "PASSIVE_DENSE_RING",
                    "count": 4,
                    "sensor_profile": "PASSIVE_MAD_ONLY",
                },
                "formations": {
                    "PASSIVE_DENSE_RING": {
                        "arc_degrees": 360,
                        "offset_bearing": "omnidirectional",
                        "standoff_km_min": 5,
                        "standoff_km_max": 10,
                    }
                },
            },
        },
    ]

    gen = _proc(
        "Load demo QUEUED orders",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        STD_BUNDLE,
        -1200,
        0,
        {
            "character-set": "UTF-8",
            "File Size": "0B",
            "Batch Size": "1",
            "Unique FlowFiles": "false",
            "Data Format": "Text",
            "Custom Text": json.dumps(demo_orders, ensure_ascii=False),
        },
        schedule="30 sec",
    )
    executor = _proc(
        "MissionOrderExecutor",
        "MissionOrderExecutor",
        PY_BUNDLE,
        -800,
        0,
        {
            "Max Active Missions": "8",
            "Tick Minutes": "5",
            "Process Queued Orders": "true",
        },
        py=True,
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
    )
    split_bundle = _proc(
        "MissionBundleSplit",
        "MissionBundleSplit",
        PY_BUNDLE,
        -400,
        0,
        {},
        py=True,
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
    )
    split_lines = _proc(
        "SplitText missions (parallel)",
        "org.apache.nifi.processors.standard.SplitText",
        STD_BUNDLE,
        0,
        0,
        {
            "Line Split Count": "1",
            "Header Line Count": "0",
            "Remove Trailing Newlines": "true",
        },
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
    )
    buoy = _proc(
        "BuoySensorSimulator MISSION_PLAYBOOK",
        "BuoySensorSimulator",
        PY_BUNDLE,
        400,
        -200,
        {
            "Number of Records per FlowFile": "1",
            "Deployment Mode": "MISSION_PLAYBOOK",
        },
        py=True,
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
        auto_term=["failure"],
    )
    gen_marine = _proc(
        "Marine tick",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        STD_BUNDLE,
        -1200,
        280,
        {
            "character-set": "UTF-8",
            "File Size": "0B",
            "Batch Size": "1",
            "Unique FlowFiles": "false",
            "Data Format": "Text",
        },
        schedule="30 sec",
    )
    marine = _proc(
        "MarineShipSimulator (NBA runtime)",
        "MarineShipSimulator",
        PY_BUNDLE,
        -800,
        280,
        {"Number of Ships": "5"},
        py=True,
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
    )
    stanag = _proc(
        "StanagMessageGenerator (mission tags)",
        "StanagMessageGenerator",
        PY_BUNDLE,
        400,
        360,
        {
            "Number of Messages": "6",
            "Traffic Profile": "watch_section",
            "Output Format": "ndjson",
            "Require Marine Ship Positions": "true",
        },
        py=True,
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
        auto_term=["failure"],
    )
    log_buoy = _proc(
        "Log buoy success",
        "org.apache.nifi.processors.standard.LogAttribute",
        STD_BUNDLE,
        800,
        -200,
        {"Log Level": "info", "Attributes to Log": "mission.id,mission.buoy.count"},
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
        auto_term=["success"],
    )
    log_stanag = _proc(
        "Log STANAG mission.id",
        "org.apache.nifi.processors.standard.LogAttribute",
        STD_BUNDLE,
        800,
        360,
        {"Log Level": "info", "Attributes to Log": "mission.id,stanag.message.count"},
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
        auto_term=["success"],
    )
    log_marine = _proc(
        "Log marine tick",
        "org.apache.nifi.processors.standard.LogAttribute",
        STD_BUNDLE,
        -400,
        280,
        {"Log Level": "info", "Attributes to Log": "ship.simulation"},
        schedule="0 sec",
        strategy="EVENT_DRIVEN",
        auto_term=["success"],
    )

    procs = [
        gen,
        executor,
        split_bundle,
        split_lines,
        buoy,
        gen_marine,
        marine,
        stanag,
        log_buoy,
        log_stanag,
        log_marine,
    ]
    for p in procs:
        p["groupIdentifier"] = pg_id

    connections = [
        _conn(pg_id, gen, executor, ["success"]),
        _conn(pg_id, executor, split_bundle, ["success"]),
        _conn(pg_id, split_bundle, split_lines, ["success"]),
        _conn(pg_id, gen_marine, marine, ["success"]),
        _conn(pg_id, split_lines, buoy, ["splits"], [{"x": 200, "y": -80}]),
        _conn(pg_id, split_lines, stanag, ["splits"], [{"x": 200, "y": 200}]),
        _conn(pg_id, buoy, log_buoy, ["success"]),
        _conn(pg_id, stanag, log_stanag, ["success"]),
        _conn(pg_id, marine, log_marine, ["success"]),
    ]

    return {
        "identifier": pg_id,
        "instanceIdentifier": None,
        "name": "Mission Execution",
        "comments": "NBA parallel missions: executor → split → buoy / marine / STANAG. Sync nifi-processors/upload first.",
        "position": {"x": 3200.0, "y": 520.0},
        "processGroups": [],
        "remoteProcessGroups": [],
        "processors": procs,
        "connections": connections,
        "inputPorts": [],
        "outputPorts": [],
        "labels": [
            {
                "identifier": _id(),
                "instanceIdentifier": None,
                "position": {"x": -1220, "y": -120},
                "size": {"height": 80, "width": 400},
                "style": {"font-size": "14px"},
                "value": "Mission Execution (NiFi2 / Python 1.1.0)\nParallel paths after SplitText",
                "componentType": "LABEL",
                "groupIdentifier": pg_id,
            }
        ],
        "funnels": [],
        "controllerServices": [],
        "defaultFlowFileExpiration": "0 sec",
        "defaultBackPressureObjectThreshold": 10000,
        "defaultBackPressureDataSizeThreshold": "1 GB",
        "componentType": "PROCESS_GROUP",
        "flowFileConcurrency": "UNBOUNDED",
        "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
        "parameterContextName": None,
        "parameterContext": None,
        "logFileSuffix": "",
    }


def main() -> None:
    pg = build_mission_execution_pg()
    root = {
        "flowContents": {
            "identifier": _id(),
            "instanceIdentifier": None,
            "name": "Mission_Execution",
            "comments": "Import as standalone flow or drag PG into proj_triton_showcase",
            "position": {"x": 0, "y": 0},
            "processGroups": [pg],
            "remoteProcessGroups": [],
            "processors": [],
            "connections": [],
            "inputPorts": [],
            "outputPorts": [],
            "labels": [],
            "funnels": [],
            "controllerServices": [],
            "defaultFlowFileExpiration": "0 sec",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "componentType": "PROCESS_GROUP",
            "flowFileConcurrency": "UNBOUNDED",
            "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
        }
    }
    from pathlib import Path

    out_path = Path(__file__).resolve().parent / "Mission_Execution-version-1.json"
    out_path.write_text(json.dumps(root, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()