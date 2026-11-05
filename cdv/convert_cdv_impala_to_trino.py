#!/usr/bin/env python3
"""Convert a Cloudera Data Visualization export from Impala-style names to Trino.

Rewrites table references ``defense.<table>`` → ``iceberg.defense.<table>`` everywhere
in the export JSON (dataset_detail, dataset_tablenames, embedded SQL, report_data, …).

Usage:
  python3 convert_cdv_impala_to_trino.py input.json output.json
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

# Do not double-prefix if already qualified.
_DEFENSE_TABLE = re.compile(r"(?<!iceberg\.)defense\.([a-z][a-z0-9_]*)", re.IGNORECASE)


def _qualify_trino(text: str) -> str:
    return _DEFENSE_TABLE.sub(r"iceberg.defense.\1", text)


def _walk(obj: Any) -> Any:
    if isinstance(obj, str):
        return _qualify_trino(obj)
    if isinstance(obj, list):
        return [_walk(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _walk(v) for k, v in obj.items()}
    return obj


def convert(data: dict) -> dict:
    out = _walk(data)
    version = out.setdefault("version", {})
    if isinstance(version, dict):
        version["Trino qualifier conversion"] = "defense.* → iceberg.defense.*"
    return out


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    with src.open(encoding="utf-8") as f:
        data = json.load(f)
    converted = convert(data)
    with dst.open("w", encoding="utf-8") as f:
        json.dump(converted, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {dst} ({dst.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
