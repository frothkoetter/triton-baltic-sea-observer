# CML Experiment — trains model.joblib / model.pkl for predict.py
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
os.chdir(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

import train

train.main()