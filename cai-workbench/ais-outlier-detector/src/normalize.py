"""Normalize AIS NDJSON / JSON records to a common schema."""

from __future__ import annotations

from typing import Any, Dict, List


def _pick(record: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in record and record[key] is not None:
            return record[key]
    return default


def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return a dict with canonical keys used by rules + ML."""
    lat = float(_pick(raw, "Latitude", "latitude", default=0.0))
    lon = float(_pick(raw, "Longitude", "longitude", default=0.0))
    dest = _pick(raw, "Destination", "destination", default="")
    if isinstance(dest, dict):
        dest = dest.get("name", "")
    return {
        "MMSI": int(_pick(raw, "MMSI", "mmsi", default=0)),
        "Event_Timestamp": str(_pick(raw, "Event_Timestamp", "event_timestamp", default="")),
        "Latitude": lat,
        "Longitude": lon,
        "Speed": float(_pick(raw, "Speed", "speed", default=0.0)),
        "Course": float(_pick(raw, "Course", "course", default=0.0)),
        "Status": str(_pick(raw, "Status", "status", default="")),
        "Destination": str(dest),
    }


def parse_input_payload(payload: Any) -> List[Dict[str, Any]]:
    """Accept CML args dict, a list, or NDJSON string."""
    if isinstance(payload, dict):
        if "records" in payload:
            rows = payload["records"]
        elif "ndjson" in payload:
            text = str(payload["ndjson"])
            rows = [_parse_line(line) for line in text.splitlines() if line.strip()]
        elif "json" in payload:
            import json

            parsed = json.loads(payload["json"])
            rows = parsed if isinstance(parsed, list) else [parsed]
        else:
            rows = [payload]
    elif isinstance(payload, list):
        rows = payload
    elif isinstance(payload, str):
        import json

        text = payload.strip()
        if text.startswith("["):
            rows = json.loads(text)
        else:
            rows = [_parse_line(line) for line in text.splitlines() if line.strip()]
    else:
        raise ValueError("Unsupported AIS input type")

    return [normalize_record(r) for r in rows if isinstance(r, dict)]


def _parse_line(line: str) -> Dict[str, Any]:
    import json

    return json.loads(line)


def format_outlier(row: Dict[str, Any]) -> Dict[str, Any]:
    """Public JSON shape for NiFi / Impala vessel_outliers."""
    return {
        "MMSI": row["MMSI"],
        "Event_Timestamp": row["Event_Timestamp"],
        "Longitude": round(float(row["Longitude"]), 5),
        "Latitude": round(float(row["Latitude"]), 5),
        "Speed": round(float(row["Speed"]), 1),
        "Course": round(float(row["Course"]), 1),
        "Status": row["Status"],
        "Destination": row["Destination"],
        "Outlier_Type": row["Outlier_Type"],
        "Reason": row["Reason"],
        "Recommendation": row["Recommendation"],
        "Severity_Level": int(row["Severity_Level"]),
    }
