# Cloudera AI — use the CML template flow

Do **not** step through `deploy_cml_model.py` line by line.

## Train

Run an **Experiment** with **`fit.py`**, or one session cell:

```python
import os, runpy
runpy.run_path(os.path.join(os.environ["CDSW_WORKING_DIRECTORY"], "fit.py"), run_name="__main__")
```

## Deploy

1. Models → Deploy Model → **`predict.py`** → function **`predict`**
2. Requires `model.pkl` from a successful `fit.py` run.

## Terminal

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 fit.py
```
