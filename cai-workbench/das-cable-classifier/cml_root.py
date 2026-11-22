"""Resolve das-cable-classifier root when CML deploy cwd is /home/cdsw."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_model_root() -> Path:
    mod = globals().get("__file__")
    if mod:
        root = Path(mod).resolve().parent
        if _has_model(root):
            return root

    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    candidates: list[Path] = []
    if work and work not in ("", "/"):
        base = Path(work)
        candidates.extend(
            [
                base / "cai-workbench" / "das-cable-classifier",
                base / "das-cable-classifier",
                base,
            ]
        )
    home = Path.home()
    candidates.extend(
        [
            home / "cai-workbench" / "das-cable-classifier",
            Path("/home/cdsw") / "cai-workbench" / "das-cable-classifier",
            home,
            Path("/home/cdsw"),
            Path.cwd(),
        ]
    )
    seen: set[Path] = set()
    for c in candidates:
        try:
            cr = c.resolve()
        except OSError:
            continue
        if cr in seen:
            continue
        seen.add(cr)
        if _has_model(cr):
            return cr

    raise FileNotFoundError(
        "model.pkl not found — run train.py or fit.py, or ensure bundled/model.pkl "
        "is present and cdsw-build.sh ran (copies bundled → model.pkl). "
        f"CDSW_WORKING_DIRECTORY={work or '(unset)'} cwd={Path.cwd()}"
    )


def model_pkl_path() -> Path:
    root = resolve_model_root()
    direct = root / "model.pkl"
    if direct.is_file():
        return direct
    bundled = root / "bundled" / "model.pkl"
    if bundled.is_file():
        return bundled
    raise FileNotFoundError(f"No model.pkl under {root}")


def _has_model(root: Path) -> bool:
    return (root / "model.pkl").is_file() or (root / "bundled" / "model.pkl").is_file()


def install_root(root: Path | None = None) -> Path:
    root = root or resolve_model_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root