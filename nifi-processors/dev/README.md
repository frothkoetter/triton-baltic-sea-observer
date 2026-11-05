# Local development (not for NiFi upload)

```bash
cd nifi-processors/dev
python3 -m pip install -r requirements-dev.txt
PYTHONPATH=../upload python3 export_vessel_samples.py
PYTHONPATH=../upload python3 ../upload/LidarSimulator.py
```

- `requirements-dev.txt` — laspy, pyproj, faker, etc.
- `export_vessel_samples.py` — write LAS samples to `samples/`
- `simulate-ships.py` — legacy NiFi processor (`simulateShips`); use `upload/ShipSimulationRoutesGemini.py` in production instead.
