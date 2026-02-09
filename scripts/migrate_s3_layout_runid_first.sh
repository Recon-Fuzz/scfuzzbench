#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' >&2
Migrate scfuzzbench S3 layout to run-id-first prefixes.

Old layout:
  logs/<benchmark_uuid>/<run_id>/...
  corpus/<benchmark_uuid>/<run_id>/... (optional)

New layout:
  logs/<run_id>/<benchmark_uuid>/...
  corpus/<run_id>/<benchmark_uuid>/... (optional)

This script also copies each run's manifest to the docs index:
  runs/<run_id>/<benchmark_uuid>/manifest.json

Usage:
  BUCKET=... ./scripts/migrate_s3_layout_runid_first.sh [--profile NAME] [--execute]

Defaults to --dryrun (no changes). Pass --execute to perform the migration.
EOF
}

BUCKET="${BUCKET:-}"
PROFILE=""
EXECUTE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --execute)
      EXECUTE=1
      shift
      ;;
    --dryrun)
      EXECUTE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${BUCKET}" ]]; then
  echo "error: missing BUCKET (set env BUCKET=... or pass --bucket ...)" >&2
  exit 2
fi

AWS=(aws)
if [[ -n "${PROFILE}" ]]; then
  AWS+=(--profile "${PROFILE}")
fi

MV_ARGS=(--recursive --no-progress)
CP_ARGS=(--no-progress)
if (( EXECUTE == 0 )); then
  MV_ARGS+=(--dryrun)
  CP_ARGS+=(--dryrun)
  echo "DRY RUN (no changes will be made). Pass --execute to apply." >&2
else
  echo "EXECUTE MODE (this will move objects in S3)." >&2
fi

echo "Discovering old-layout runs under s3://${BUCKET}/logs/<benchmark_uuid>/<run_id>/..." >&2

benchmarks=$(
  "${AWS[@]}" s3api list-objects-v2 \
    --bucket "${BUCKET}" \
    --prefix "logs/" \
    --delimiter "/" \
    --output json \
    | python3 - <<'PY'
import json
import re
import sys

uuid = re.compile(r"^[0-9a-f]{32}$")
data = json.load(sys.stdin)
for entry in data.get("CommonPrefixes", []):
    prefix = entry["Prefix"].rstrip("/")
    parts = prefix.split("/")
    if len(parts) == 2 and parts[0] == "logs" and uuid.match(parts[1]):
        print(parts[1])
PY
)

if [[ -z "${benchmarks}" ]]; then
  echo "No old-layout benchmarks found under logs/. Nothing to do." >&2
  exit 0
fi

# Collect (benchmark_uuid, run_id) pairs up front so listing doesn't race with moves.
pairs=$(
  while read -r bench; do
    [[ -z "${bench}" ]] && continue
    "${AWS[@]}" s3api list-objects-v2 \
      --bucket "${BUCKET}" \
      --prefix "logs/${bench}/" \
      --delimiter "/" \
      --output json \
      | python3 - <<'PY'
import json
import sys

data = json.load(sys.stdin)
for entry in data.get("CommonPrefixes", []):
    prefix = entry["Prefix"].rstrip("/")
    parts = prefix.split("/")
    if len(parts) == 3 and parts[0] == "logs" and parts[2].isdigit():
        # parts[1] is benchmark_uuid, parts[2] is run_id
        print(f"{parts[1]} {parts[2]}")
PY
  done <<<"${benchmarks}"
)

if [[ -z "${pairs}" ]]; then
  echo "No old-layout runs found under logs/<benchmark_uuid>/. Nothing to do." >&2
  exit 0
fi

echo "Planned migrations (benchmark_uuid run_id):" >&2
echo "${pairs}" | python3 - <<'PY'
import sys

rows = []
for line in sys.stdin.read().splitlines():
    if not line.strip():
        continue
    bench, run_id = line.split()
    rows.append((int(run_id), bench))
for run_id, bench in sorted(rows):
    print(f"- {bench} {run_id}", file=sys.stderr)
PY

echo "" >&2
echo "Migrating logs + corpus (if present), and writing runs/ index manifests..." >&2

echo "${pairs}" | python3 - <<'PY'
import sys

rows = []
for line in sys.stdin.read().splitlines():
    if not line.strip():
        continue
    bench, run_id = line.split()
    rows.append((int(run_id), bench))
for run_id, bench in sorted(rows):
    print(f"{bench} {run_id}")
PY \
  | while read -r bench run_id; do
      [[ -z "${bench}" || -z "${run_id}" ]] && continue

      src_logs="s3://${BUCKET}/logs/${bench}/${run_id}/"
      dst_logs="s3://${BUCKET}/logs/${run_id}/${bench}/"
      echo "" >&2
      echo "logs:  ${src_logs} -> ${dst_logs}" >&2
      "${AWS[@]}" s3 mv "${src_logs}" "${dst_logs}" "${MV_ARGS[@]}"

      # corpus is optional; only move if the source prefix has any objects.
      corpus_count=$("${AWS[@]}" s3api list-objects-v2 \
        --bucket "${BUCKET}" \
        --prefix "corpus/${bench}/${run_id}/" \
        --max-keys 1 \
        --query 'KeyCount' \
        --output text)
      if [[ "${corpus_count}" != "0" ]]; then
        src_corpus="s3://${BUCKET}/corpus/${bench}/${run_id}/"
        dst_corpus="s3://${BUCKET}/corpus/${run_id}/${bench}/"
        echo "corpus: ${src_corpus} -> ${dst_corpus}" >&2
        "${AWS[@]}" s3 mv "${src_corpus}" "${dst_corpus}" "${MV_ARGS[@]}"
      else
        echo "corpus: (none)" >&2
      fi

      # Ensure docs index exists for this run.
      manifest_key="logs/${run_id}/${bench}/manifest.json"
      if "${AWS[@]}" s3api head-object --bucket "${BUCKET}" --key "${manifest_key}" >/dev/null 2>&1; then
        src_manifest="s3://${BUCKET}/${manifest_key}"
        dst_manifest="s3://${BUCKET}/runs/${run_id}/${bench}/manifest.json"
        echo "index: ${src_manifest} -> ${dst_manifest}" >&2
        "${AWS[@]}" s3 cp "${src_manifest}" "${dst_manifest}" "${CP_ARGS[@]}"
      else
        echo "index: missing manifest at ${manifest_key} (skipping)" >&2
      fi
    done

echo "" >&2
echo "Done. If you ran with --execute, the old layout prefixes should now be empty." >&2
