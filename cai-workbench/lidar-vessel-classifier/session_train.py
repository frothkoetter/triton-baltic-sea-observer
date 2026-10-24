"""
Cloudera AI Python session entry (Cell In[n]) — no __file__ required.

Paste this whole file into one cell and run it, or in Terminal:
  python3 session_train.py
"""

import os
import sys

ROOT = os.environ.get("CDSW_WORKING_DIRECTORY") or os.environ.get("LIDAR_PROJECT_ROOT")
if not ROOT:
    raise SystemExit(
        "Set CDSW_WORKING_DIRECTORY to your lidar-vessel-classifier project folder "
        "(Project Settings → Environment)."
    )

os.chdir(ROOT)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import train

train.main()
