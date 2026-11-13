#!/usr/bin/env bash
# Draft: direkter WFS-Download (curl) oder Python-Normalisierung mit WKT
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/data/emodnet"
mkdir -p "$OUT"

if command -v python3 >/dev/null && python3 -c "import requests, shapely, pyarrow" 2>/dev/null; then
  echo "Using sync_emodnet_baltic.py → Parquet + GeoJSON + WKT"
  python3 "$ROOT/scripts/sync_emodnet_baltic.py" --output-dir "$OUT"
  exit 0
fi

BBOX="9.0,53.5,30.5,66.0,EPSG:4326"
BASE="https://ows.emodnet-humanactivities.eu/wfs"

curl -fsSG "$BASE" \
  --data-urlencode "service=WFS" \
  --data-urlencode "version=2.0.0" \
  --data-urlencode "request=GetFeature" \
  --data-urlencode "typeName=emodnet:pipelines" \
  --data-urlencode "outputFormat=application/json" \
  --data-urlencode "srsName=EPSG:4326" \
  --data-urlencode "bbox=$BBOX" \
  -o "$OUT/ostsee_pipelines_emodnet.geojson"

curl -fsSG "$BASE" \
  --data-urlencode "service=WFS" \
  --data-urlencode "version=2.0.0" \
  --data-urlencode "request=GetFeature" \
  --data-urlencode "typeName=emodnet:cables" \
  --data-urlencode "outputFormat=application/json" \
  --data-urlencode "srsName=EPSG:4326" \
  --data-urlencode "bbox=$BBOX" \
  -o "$OUT/ostsee_cables_emodnet.geojson"

if command -v jq >/dev/null; then
  jq -c . "$OUT/ostsee_pipelines_emodnet.geojson" > "$OUT/ostsee_pipelines_trino.geojson"
  jq -c . "$OUT/ostsee_cables_emodnet.geojson" > "$OUT/ostsee_cables_trino.geojson"
fi

echo "GeoJSON in $OUT (install requests shapely pyarrow for WKT Parquet export)"
