# Bundled classifier

`model.pkl` is a sklearn `Pipeline` (StandardScaler + RandomForestClassifier) trained on sklearn **1.7.2**, with 12 LiDAR features. The bundled snapshot may list **9** labels until you re-run `fit.py` on `data/lidar/` (now **10** files including `fishing_boat`).

Baseline 9: `container_ship`, `corvette`, `ferry`, `frigate`, `gas_tanker`, `landing_ship`, `oil_tanker`, `sailing`, `submarine`. After retrain: add `fishing_boat`.

`cdsw-build.sh` copies these files to the project root as `model.pkl` and `label_names.json` when `model.pkl` is not already present.
