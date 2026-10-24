# NiFi Python processors

These scripts run inside Apache NiFi (Python processors) or locally for development.

## Local setup (LiDAR simulator and others)

From the repository root, with your virtualenv activated:

```bash
cd nifi-processors
python3 -m pip install -r requirements.txt
python3 LidarSimulator.py
```

`LidarSimulator.py` needs **laspy** and **pyproj** (listed in `requirements.txt`).

| Error | Fix |
|-------|-----|
| `No module named 'laspy'` | `python3 -m pip install -r requirements.txt` |
| `No module named 'pyproj'` but pip installed **pyroj** | Wrong package — `requirements.txt` line 5 must be `pyproj`, not `pyroj`. Run: `python3 -m pip uninstall pyroj -y && python3 -m pip install pyproj` |

Output: `baltic_fleet.las` in the current directory.

## NiFi runtime

On NiFi, declare processor dependencies in each processor’s `ProcessorDetails.dependencies` (see `LidarSimulator`). NiFi installs those packages for the Python processor bundle; a local venv is only for offline testing.
