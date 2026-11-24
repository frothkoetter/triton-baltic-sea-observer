#!/usr/bin/env bash
#
# Download today's SITREP PDFs, LiDAR (LAS/LAZ), and DAS (HDF5/WAV) from S3 with AWS CLI,
# store under a local directory, and compress to .tar.gz.
#
# Auth: AWS SSO (recommended) via --profile / AWS_PROFILE, or workload keys (S3_WORKLOAD_*).
# S3_ENDPOINT_URL / AWS_ENDPOINT_URL are optional (omit when SSO profile reaches the bucket).
# Prefixes: SITREP_S3_PREFIX or LAGEBILD_S3_PREFIX, S3_LIDAR_ALLOWED_PREFIX, S3_DAS_ALLOWED_PREFIX.
#
# Usage:
#   aws sso login --profile cdw-dev
#   ./scripts/download-today-s3-artifacts.sh --profile cdw-dev
#   ./scripts/download-today-s3-artifacts.sh --date 2026-11-24 --out ./artifacts-heute
#
# macOS /bin/bash is 3.2 — this script avoids bash 4-only features (e.g. mapfile).
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ARTIFACT_TZ="${ARTIFACT_TZ:-Europe/Berlin}"
OUT_DIR=""
TARGET_DATE=""
ENV_FILE=""
AWS_PROFILE_ARG=""
DRY_RUN=0
SKIP_ARCHIVE=0
SSO_LOGIN=0
QUIET_AWS=1

# download summary (label -> count)
declare -a SUMMARY_LABELS=()
declare -a SUMMARY_COUNTS=()

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  echo ""
  echo "Options:"
  echo "  --env-file PATH   shell-source KEY=VALUE file before run"
  echo "  --profile NAME    AWS CLI profile (SSO or static); sets AWS_PROFILE"
  echo "  --sso-login       run 'aws sso login' for --profile / AWS_PROFILE first"
  echo "  --date YYYY-MM-DD calendar day in ARTIFACT_TZ (default: today)"
  echo "  --out DIR         output directory (default: ./triton-s3-artifacts-YYYYMMDD)"
  echo "  --tz ZONE         IANA tz for --date (default: Europe/Berlin)"
  echo "  --dry-run         list objects only, no download"
  echo "  --no-archive      skip .tar.gz"
  echo "  --verbose-aws     show AWS CLI transfer progress per file (noisy)"
  echo "  -h, --help"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --profile)
      AWS_PROFILE_ARG="$2"
      shift 2
      ;;
    --sso-login)
      SSO_LOGIN=1
      shift
      ;;
    --date)
      TARGET_DATE="$2"
      shift 2
      ;;
    --out)
      OUT_DIR="$2"
      shift 2
      ;;
    --tz)
      ARTIFACT_TZ="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --no-archive)
      SKIP_ARCHIVE=1
      shift
      ;;
    --verbose-aws)
      QUIET_AWS=0
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Env file not found: $ENV_FILE" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Install AWS CLI v2 (CML: often preinstalled)." >&2
  exit 1
fi

if [[ -n "$AWS_PROFILE_ARG" ]]; then
  export AWS_PROFILE="$AWS_PROFILE_ARG"
fi

AWS_ARGS=()
if [[ -n "${AWS_PROFILE:-}" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
fi
if [[ -n "${S3_ENDPOINT_URL:-}" ]]; then
  AWS_ARGS+=(--endpoint-url "$S3_ENDPOINT_URL")
elif [[ -n "${AWS_ENDPOINT_URL:-}" ]]; then
  AWS_ARGS+=(--endpoint-url "$AWS_ENDPOINT_URL")
fi

aws_cli() {
  aws "${AWS_ARGS[@]}" "$@"
}

ensure_aws_credentials() {
  if [[ "$SSO_LOGIN" == 1 ]]; then
    if [[ -z "${AWS_PROFILE:-}" ]]; then
      echo "Option --sso-login requires --profile or AWS_PROFILE" >&2
      exit 1
    fi
    echo "== AWS SSO login (profile: $AWS_PROFILE) =="
    aws sso login --profile "$AWS_PROFILE"
  fi
  if ! aws_cli sts get-caller-identity --output text >/dev/null 2>&1; then
    echo "AWS credentials not valid. For SSO run:" >&2
    echo "  aws sso login --profile YOUR_PROFILE" >&2
    echo "  $0 --profile YOUR_PROFILE [--sso-login]" >&2
    exit 1
  fi
  local who
  who="$(aws_cli sts get-caller-identity --query Arn --output text 2>/dev/null || true)"
  echo "AWS identity: ${who:-ok}"
}

progress_line() {
  printf '%s\n' "$*" >&2
}

human_bytes() {
  local n="${1:-0}"
  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec-i --suffix=B "$n" 2>/dev/null || echo "${n}B"
  else
    echo "${n} bytes"
  fi
}

if [[ -z "$TARGET_DATE" ]]; then
  TARGET_DATE="$(TZ="$ARTIFACT_TZ" date +%F)"
fi

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="${ROOT}/triton-s3-artifacts-${TARGET_DATE//-/}"
fi

mkdir -p "$OUT_DIR"

SITREP_PREFIX="${SITREP_S3_PREFIX:-${LAGEBILD_S3_PREFIX:-}}"
LIDAR_PREFIX="${S3_LIDAR_ALLOWED_PREFIX:-}"
DAS_PREFIX="${S3_DAS_ALLOWED_PREFIX:-}"

export ARTIFACT_TZ TARGET_DATE OUT_DIR DRY_RUN AWS_PROFILE

ensure_aws_credentials

parse_s3_uri() {
  local uri="$1"
  uri="${uri#s3a://}"
  uri="${uri#s3://}"
  if [[ -z "$uri" ]]; then
    echo "ERR empty s3 uri" >&2
    return 1
  fi
  local bucket key=""
  if [[ "$uri" == */* ]]; then
    bucket="${uri%%/*}"
    key="${uri#*/}"
    [[ -n "$key" && "$key" != */ ]] && key="${key}/"
  else
    bucket="$uri"
  fi
  printf '%s\t%s\n' "$bucket" "$key"
}

# Lists keys under prefix whose LastModified falls on TARGET_DATE in ARTIFACT_TZ.
# Args: bucket prefix extension1 extension2 ...
list_today_keys() {
  local bucket="$1"
  local prefix="$2"
  shift 2
  python3 - "$bucket" "$prefix" "$@" <<'PY'
import json, os, subprocess, sys
from datetime import datetime
from zoneinfo import ZoneInfo

bucket, prefix = sys.argv[1], sys.argv[2]
exts = tuple(e.lower().lstrip(".") for e in sys.argv[3:] if e)
day = os.environ["TARGET_DATE"]
tz = ZoneInfo(os.environ.get("ARTIFACT_TZ", "Europe/Berlin"))
start = datetime.fromisoformat(f"{day}T00:00:00").replace(tzinfo=tz)
end = datetime.fromisoformat(f"{day}T23:59:59.999999").replace(tzinfo=tz)

aws_cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix, "--output", "json"]
profile = (os.environ.get("AWS_PROFILE") or "").strip()
if profile:
    aws_cmd[1:1] = ["--profile", profile]
endpoint = os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
if endpoint:
    insert = 3 if profile else 1
    aws_cmd[insert:insert] = ["--endpoint-url", endpoint]

token = None
page = 0
scanned = 0
matched = 0
while True:
    page += 1
    cmd = list(aws_cmd)
    if token:
        cmd.extend(["--continuation-token", token])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr or proc.stdout)
        sys.exit(proc.returncode)
    data = json.loads(proc.stdout or "{}")
    batch = data.get("Contents") or []
    scanned += len(batch)
    for obj in batch:
        key = obj.get("Key") or ""
        if exts:
            low = key.lower()
            if not any(low.endswith(f".{e}") for e in exts):
                continue
        lm = obj.get("LastModified")
        if not lm:
            continue
        dt = datetime.fromisoformat(lm.replace("Z", "+00:00"))
        if start <= dt.astimezone(tz) <= end:
            matched += 1
            print(key)
    print(
        f"  … list page {page}: scanned {scanned}, matched {matched} for {day}",
        file=sys.stderr,
        flush=True,
    )
    if not data.get("IsTruncated"):
        break
    token = data.get("NextContinuationToken")
PY
}

download_key() {
  local bucket="$1"
  local key="$2"
  local dest_dir="$3"
  local key_prefix="$4"
  local index="$5"
  local total="$6"
  local rel="${key#"${key_prefix:-}"}"
  local dest="${dest_dir}/${rel}"
  mkdir -p "$(dirname "$dest")"
  if [[ "$DRY_RUN" == 1 ]]; then
    progress_line "  [${index}/${total}] would download ${rel}"
    return 0
  fi
  progress_line "  [${index}/${total}] downloading ${rel} …"
  local -a cp_flags=()
  if [[ "$QUIET_AWS" == 1 ]]; then
    cp_flags+=(--only-show-errors --no-progress)
  fi
  aws_cli s3 cp "s3://${bucket}/${key}" "$dest" "${cp_flags[@]}"
  progress_line "  [${index}/${total}] OK ${rel}"
}

download_category() {
  local label="$1"
  local uri="$2"
  shift 2
  local subdir="$1"
  shift
  if [[ -z "$uri" ]]; then
    echo "SKIP $label (prefix not set)"
    return 0
  fi
  local parsed bucket prefix
  parsed="$(parse_s3_uri "$uri")" || return 0
  bucket="${parsed%%$'\t'*}"
  prefix="${parsed#*$'\t'}"
  local dest="${OUT_DIR}/${subdir}"
  mkdir -p "$dest"
  echo "== $label: s3://${bucket}/${prefix} (day ${TARGET_DATE} ${ARTIFACT_TZ}) =="
  progress_line "  Listing objects…"
  local keys=()
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] && keys+=("$line")
  done < <(list_today_keys "$bucket" "$prefix" "$@" || true)
  local n=${#keys[@]}
  SUMMARY_LABELS+=("$label")
  SUMMARY_COUNTS+=("$n")
  if [[ "$n" -eq 0 ]]; then
    echo "  (no objects for this day)"
    return 0
  fi
  echo "  Found $n file(s) to download"
  local i=0
  for key in "${keys[@]}"; do
    i=$((i + 1))
    download_key "$bucket" "$key" "$dest" "$prefix" "$i" "$n"
  done
  echo "  Done $label: $n file(s)"
}

# SITREP index: also pull index.json when any PDF matches (for metadata)
fetch_sitrep_index() {
  local uri="$SITREP_PREFIX"
  [[ -z "$uri" ]] && return 0
  local parsed bucket prefix
  parsed="$(parse_s3_uri "$uri")" || return 0
  bucket="${parsed%%$'\t'*}"
  prefix="${parsed#*$'\t'}"
  local dest="${OUT_DIR}/sitrep/index.json"
  mkdir -p "${OUT_DIR}/sitrep"
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "  would cp s3://${bucket}/${prefix}index.json"
    return 0
  fi
  if aws_cli s3 cp "s3://${bucket}/${prefix}index.json" "$dest" --only-show-errors 2>/dev/null; then
    echo "  OK sitrep/index.json"
  fi
}

MANIFEST="${OUT_DIR}/manifest.txt"
{
  echo "triton-s3-artifacts"
  echo "date=${TARGET_DATE} tz=${ARTIFACT_TZ}"
  echo "created=$(TZ=UTC date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "sitrep_prefix=${SITREP_PREFIX:-}"
  echo "lidar_prefix=${LIDAR_PREFIX:-}"
  echo "das_prefix=${DAS_PREFIX:-}"
  echo "aws_profile=${AWS_PROFILE:-}"
} >"$MANIFEST"

echo "Output: $OUT_DIR"
download_category "SITREP PDF" "$SITREP_PREFIX" "sitrep" "pdf"
fetch_sitrep_index
download_category "LiDAR" "$LIDAR_PREFIX" "lidar" "las" "laz"
download_category "DAS" "$DAS_PREFIX" "das" "h5" "hdf5" "hdf" "wav"

if [[ "$DRY_RUN" == 1 ]]; then
  echo "Dry run complete."
  exit 0
fi

find "$OUT_DIR" -type f ! -name manifest.txt | sort >>"$MANIFEST"

total_files=0
for c in "${SUMMARY_COUNTS[@]}"; do
  total_files=$((total_files + c))
done

OUT_BYTES=0
if command -v du >/dev/null 2>&1; then
  OUT_BYTES=$(du -sb "$OUT_DIR" 2>/dev/null | cut -f1 || echo 0)
fi

echo "== Summary (${TARGET_DATE}) =="
for idx in "${!SUMMARY_LABELS[@]}"; do
  echo "  ${SUMMARY_LABELS[$idx]}: ${SUMMARY_COUNTS[$idx]} file(s)"
done
echo "  Total: ${total_files} file(s), $(human_bytes "$OUT_BYTES") under $(basename "$OUT_DIR")"

ARCHIVE="${OUT_DIR}.tar.gz"
if [[ "$SKIP_ARCHIVE" == 0 ]]; then
  echo "== Compress: $ARCHIVE =="
  progress_line "  Packing ${total_files} artifact file(s)…"
  tar -czf "$ARCHIVE" -C "$(dirname "$OUT_DIR")" "$(basename "$OUT_DIR")"
  arch_size=$(du -h "$ARCHIVE" 2>/dev/null | cut -f1 || echo "?")
  echo "Done: $ARCHIVE ($arch_size)"
else
  echo "Done: $OUT_DIR (no archive)"
fi