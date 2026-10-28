# Regression tests (not for NiFi upload)

From repository root:

```bash
cd nifi-processors
PYTHONPATH=upload:tests python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=upload:tests python3 tests/test_baltic_ship_routing.py
```

`baltic_coast_pins.py` lives here; it is only used by tests, not by runtime processors.
