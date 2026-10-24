"""
Cloudera AI Python session entry (Cell In[n]) — no __file__ required.

Paste this whole file into one cell and run it, or in Terminal:
  python3 session_train.py
"""

import os
import sys

root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.environ.get("LIDAR_PROJECT_ROOT")
if not root:
    raise SystemExit("Set CDSW_WORKING_DIRECTORY to the lidar-vessel-classifier folder.")

sys.path.insert(0, root)
os.chdir(root)

import train

train.main()
