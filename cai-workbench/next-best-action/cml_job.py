#!/usr/bin/env python3
"""
UC2 — NBA (CML Scheduled Job script).

Select **this file** in the Jobs UI when only Python scripts are allowed:
`cai-workbench/next-best-action/cml_job.py`

Configure via **project/job environment** (no CLI required):
  NBA_PERSIST_TRINO=1
  NBA_EXIT_IF_QUIET=1
  NBA_PHASE=2   (or 3 for playbook/LLM)
  NBA_DEMO_GREIFSWALD=0   (live alerts; set 1 only for Greifswald demo)
  NBA_SIM_TICKS=6
  TRINO_* / LAGEBILD_TRINO_* (same cluster as UC1)

Build: `pip install -r requirements.txt` or `bash cdsw-build.sh`
"""
from __future__ import annotations

from run_nba import _exit_job, main

if __name__ == "__main__":
    _exit_job(main())