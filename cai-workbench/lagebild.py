"""
Deprecated: Baltic situation awareness moved to ``cai-workbench/situation-awareness/``.

See ``situation-awareness/README.md`` for notebook and hourly job instructions.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

warnings.warn(
    "Import from cai-workbench/situation-awareness/lagebild.py instead.",
    DeprecationWarning,
    stacklevel=2,
)

_SA_ROOT = Path(__file__).resolve().parent / "situation-awareness"
if str(_SA_ROOT) not in sys.path:
    sys.path.insert(0, str(_SA_ROOT))

from src.pipeline import PipelineResult, run_pipeline

__all__ = ["run_pipeline", "PipelineResult"]
