# Cloudera AI: select ALL lines in this file, Run once. Do not open deploy_cml_model.py.
import os
import runpy

runpy.run_path(
    os.path.join(os.environ["CDSW_WORKING_DIRECTORY"], "deploy_now.py"),
    run_name="__main__",
)
