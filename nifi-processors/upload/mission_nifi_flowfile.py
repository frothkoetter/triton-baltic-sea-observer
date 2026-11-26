"""Helpers to read NiFi FlowFile attributes in Python processors."""

from __future__ import annotations

import json
from typing import Any


def flowfile_attr_map(flowfile: Any) -> dict[str, str]:
    get_attrs = getattr(flowfile, "getAttributes", None)
    if callable(get_attrs):
        raw = get_attrs()
        if isinstance(raw, dict):
            return {str(k): str(v) for k, v in raw.items() if v is not None}
    attrs_prop = getattr(flowfile, "attributes", None)
    if isinstance(attrs_prop, dict):
        return {str(k): str(v) for k, v in attrs_prop.items() if v is not None}
    return {}


def attr_float(attrs: dict[str, str], *keys: str, default: float | None = None) -> float | None:
    for key in keys:
        if key in attrs and attrs[key]:
            try:
                return float(attrs[key])
            except ValueError:
                pass
    return default


def attr_int(attrs: dict[str, str], *keys: str, default: int | None = None) -> int | None:
    for key in keys:
        if key in attrs and attrs[key]:
            try:
                return int(attrs[key])
            except ValueError:
                pass
    return default


def parse_mission_context(flowfile: Any) -> dict[str, Any]:
    """
    Mission dispatch context from attributes (post-split) or JSON body line.
    """
    attrs = flowfile_attr_map(flowfile)
    if attrs.get("mission.id"):
        return {
            "mission_id": attrs.get("mission.id", ""),
            "flow_uuid": attrs.get("mission.flow_uuid", ""),
            "coa_id": attrs.get("mission.coa_id", ""),
            "target_lat": attr_float(attrs, "mission.target.lat"),
            "target_lon": attr_float(attrs, "mission.target.lon"),
            "target_mode": attrs.get("mission.target.mode", "STATIC_POINT"),
            "buoy_ids": [b.strip() for b in attrs.get("mission.buoy.ids", "").split(",") if b.strip()],
            "surface_mmsis": [m.strip() for m in attrs.get("mission.surface.mmsis", "").split(",") if m.strip()],
        }
    raw = flowfile.getContentsAsBytes() if hasattr(flowfile, "getContentsAsBytes") else b""
    if not raw:
        return {}
    try:
        body = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    if isinstance(body, dict) and body.get("mission_id"):
        return body
    if isinstance(body, dict) and "attrs" in body:
        inner = body["attrs"]
        return {
            "mission_id": inner.get("mission.id", body.get("mission_id", "")),
            "target_lat": attr_float(inner, "mission.target.lat"),
            "target_lon": attr_float(inner, "mission.target.lon"),
            "buoy_ids": [b.strip() for b in inner.get("mission.buoy.ids", "").split(",") if b.strip()],
        }
    return {}