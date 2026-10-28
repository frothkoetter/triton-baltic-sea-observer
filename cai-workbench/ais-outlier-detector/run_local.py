#!/usr/bin/env python3
"""CLI: NDJSON or JSON file -> outlier JSON array on stdout."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from src.detect import detect_outliers


def main() -> int:
    text = sys.stdin.read() if len(sys.argv) < 2 else Path(sys.argv[1]).read_text()
    text = text.strip()
    if text.startswith("["):
        payload = json.loads(text)
    else:
        payload = {"ndjson": text}
    outliers = detect_outliers(payload)
    sys.stdout.write(json.dumps(outliers, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
