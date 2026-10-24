"""Deploy from a Cloudera AI Python session — run this whole file in one cell."""

import os
import sys
from pathlib import Path

root = Path(os.environ["CDSW_WORKING_DIRECTORY"]).resolve()
sys.path.insert(0, str(root))
os.chdir(root)

import deploy_cml_model

deploy_cml_model.main()
