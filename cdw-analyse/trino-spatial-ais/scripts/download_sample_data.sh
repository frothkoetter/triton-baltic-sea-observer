#!/usr/bin/env bash
# Copy or refresh AIS sample into the spatial lab (idempotent).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/data/sample_ais_positions.csv"
SRC="$ROOT/data/sample_ais_positions.csv"

if [[ ! -f "$DEST" ]]; then
  echo "Missing template $SRC" >&2
  exit 1
fi

TRACK="$ROOT/data/sample_ais_pipeline_track.csv"
echo "AIS AOI sample: $DEST ($(wc -l < "$DEST") lines)"
echo "AIS pipeline track: $TRACK ($(wc -l < "$TRACK") lines)"
echo "Trino load: python \"$ROOT/scripts/e2e_trino_ais.py\" --load-sample --load-pipeline-track --load-blackout-sample"
echo "EMODnet Open Data: \"$ROOT/scripts/download_emodnet.sh\""
