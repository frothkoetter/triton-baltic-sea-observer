# NiFi Python processors

These scripts run inside Apache NiFi (Python processors) or locally for development.

## Local setup (LiDAR simulator and others)

From the repository root, with your virtualenv activated:

```bash
cd nifi-processors
python3 -m pip install -r requirements.txt
python3 LidarSimulator.py
```

`LidarSimulator.py` needs **laspy** and **pyproj** (listed in `requirements.txt`). If you see `ModuleNotFoundError: No module named 'laspy'`, install dependencies with the command above.

Output: `baltic_fleet.las` in the current directory.

## NiFi runtime

On NiFi, declare processor dependencies in each processor’s `ProcessorDetails.dependencies` (see `LidarSimulator`). NiFi installs those packages for the Python processor bundle; a local venv is only for offline testing.
