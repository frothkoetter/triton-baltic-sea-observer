# Deploy / train in Cloudera AI Python (`Cell In[n]`)

You are **not** running a notebook, but the UI still splits files into cells.  
**Do not** open `deploy_cml_model.py` and run imports one by one — that causes `No module named 'project_root'`.

## Deploy (do this)

1. Open **`DEPLOY.py`** (not `deploy_cml_model.py`).
2. **Select all** → **Run** (one shot).

Or paste this **one** cell:

```python
import os, runpy
runpy.run_path(os.environ["CDSW_WORKING_DIRECTORY"] + "/deploy_now.py", run_name="__main__")
```

## Train

Open **`train_now.py`**, select all, run — or Terminal:

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py
```

## After git pull

Re-run project **Build** once (runs `cdsw-build.sh`) so IPython adds the project to `sys.path` on session start. Then **restart** the Python session.

## Terminal (always works)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 deploy_cml_model.py
```
