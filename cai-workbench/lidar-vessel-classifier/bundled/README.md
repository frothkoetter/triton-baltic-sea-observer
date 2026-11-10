# Bundled classifier

`model.pkl` is a sklearn `Pipeline` (StandardScaler + RandomForestClassifier) trained on sklearn **1.7.2**, with 12 LiDAR features and 9 vessel labels:

`container_ship`, `corvette`, `ferry`, `frigate`, `gas_tanker`, `landing_ship`, `oil_tanker`, `sailing`, `submarine`

`cdsw-build.sh` copies these files to the project root as `model.pkl` and `label_names.json` when `model.pkl` is not already present.
