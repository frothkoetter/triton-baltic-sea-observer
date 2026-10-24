# Cloudera AI — Python runtime (not a notebook)

In a **Python runtime session**, the interactive interpreter does **not** set `__file__` and does **not** add your project to `PYTHONPATH`. Pasting lines from `train.py` into the interpreter will fail.

## Install Python packages (once per session)

Project **build** runs `cdsw-build.sh`. If `joblib` / `sklearn` are missing in the runtime:

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 -m pip install --user -r requirements-train.txt
```

`train.py` can also auto-install from `requirements-train.txt` on first run (disable with `LIDAR_SKIP_PIP_INSTALL=1`).

## Correct: run `train.py` as a process

Use the session **Terminal** (bash), not the Python input pane:

```bash
cd "${CDSW_WORKING_DIRECTORY}"
source cdsw_py_env.sh   # optional; sets PYTHONPATH
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```

Or:

```bash
./run_train.sh
```

## CML Job (recommended for training)

| Setting | Value |
|---------|--------|
| Type | Job |
| Command | `python3 train.py --s3-prefix "s3://..."` |
| Working directory | project root (default `CDSW_WORKING_DIRECTORY`) |

## Project environment variables (UI)

In **Project → Settings → Environment variables**, add:

```text
PYTHONPATH=/home/cdsw/lidar-vessel-classifier/src:/home/cdsw/lidar-vessel-classifier
LIDAR_PROJECT_ROOT=/home/cdsw/lidar-vessel-classifier
```

Use the path shown under **Project files** if different (e.g. nested `cai-workbench/lidar-vessel-classifier`).

After that, a **new** session can do:

```python
from features import extract_features_from_las_bytes
```

## Jupyter / `%run train.py` in a session

IPython passes `-f /tmp/.../kernel-….json` in `sys.argv`. `train.py` strips those flags automatically. You can run:

```python
%run train.py
```

or `from train import main; main()` after `cd` to the project directory.

## Interactive Python only when needed

If you must use the Python runtime REPL after configuring `PYTHONPATH`:

```python
from features import extract_features_from_las_bytes
```

To **train** from REPL (still spawns a proper script run):

```python
import subprocess, os
subprocess.check_call(
    ["python3", "train.py", "--s3-prefix", "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"],
    cwd=os.environ["CDSW_WORKING_DIRECTORY"],
)
```
