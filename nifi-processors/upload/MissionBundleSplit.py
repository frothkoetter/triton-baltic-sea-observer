"""
Convert MissionOrderExecutor bundle JSON to NDJSON (one mission per line).

Downstream: SplitText on newline → parallel branches per mission.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


class MissionBundleSplit(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.1.0"
        description = (
            "Splits mission.parallel bundle into NDJSON lines for SplitText. "
            "Each line: {mission_id, attrs, target_lat, target_lon}."
        )
        dependencies = []

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = []

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        raw = flowfile.getContentsAsBytes() or b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = {}

        missions = body.get("missions") or []
        lines: list[str] = []
        for m in missions:
            if not isinstance(m, dict):
                continue
            mid = m.get("mission.id", "")
            lines.append(
                json.dumps(
                    {
                        "mission_id": mid,
                        "attrs": m,
                        "target_lat": m.get("mission.target.lat"),
                        "target_lon": m.get("mission.target.lon"),
                    },
                    ensure_ascii=False,
                )
            )
        content = "\n".join(lines) if lines else ""
        attrs = {
            "mission.parallel.count": str(len(lines)),
            "format": "missions_ndjson",
            "mime.type": "application/x-ndjson",
        }
        if len(lines) == 1:
            attrs.update(json.loads(lines[0]).get("attrs", {}))
        return FlowFileTransformResult(
            attributes=attrs,
            contents=content.encode("utf-8"),
        )