"""Run CML model registration + deployment from a Python session (Cell In[n])."""

import os
import sys

root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.environ.get("LIDAR_PROJECT_ROOT")
if not root:
    raise SystemExit("Set CDSW_WORKING_DIRECTORY to the lidar-vessel-classifier folder.")

sys.path.insert(0, root)
os.chdir(root)

import deploy_cml_model

deploy_cml_model.main()
