#!/usr/bin/env python3
"""
UC1 — hourly Lagebild (CML Scheduled Job script).

In CML Jobs, when only **.py** files can be selected, choose **this file**
(`cai-workbench/situation-awareness/cml_job.py`).

Job flags come from **environment variables** (no shell wrapper):
  LAGEBILD_TRINO_* / LAGEBILD_LLM_* / LAGEBILD_S3_UPLOAD / LAGEBILD_S3_PREFIX
  LAGEBILD_SKIP_LLM=1  optional PDF without model
  LAGEBILD_INSERT_SQL=1  optional persistence

Build step (once): `pip install -r requirements.txt` or `bash cdsw-build.sh`
"""
from __future__ import annotations

from run_hourly import _exit_job, main

if __name__ == "__main__":
    _exit_job(main())