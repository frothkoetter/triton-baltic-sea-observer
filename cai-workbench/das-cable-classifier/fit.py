# CML Experiment — trains model.pkl + bundled/model.pkl for predict.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _classifier_root() -> Path:
    here = Path(__file__).resolve().parent
    work = Path(os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd())
    for d in (
        here,
        work / "cai-workbench" / "das-cable-classifier",
        work / "das-cable-classifier",
        work,
    ):
        if (d / "train.py").is_file():
            return d
    return here


ROOT = _classifier_root()
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")]
)
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import train

train.main()