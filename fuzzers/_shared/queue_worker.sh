#!/usr/bin/env bash
set -euo pipefail

source /opt/scfuzzbench/common.sh

require_env \
  SCFUZZBENCH_RUN_ID \
  SCFUZZBENCH_BENCHMARK_UUID \
  SCFUZZBENCH_S3_BUCKET \
  SCFUZZBENCH_LOCK_NAME \
  SCFUZZBENCH_LOCK_OBJECT_KEY

SCFUZZBENCH_QUEUE_IDLE_POLLS=${SCFUZZBENCH_QUEUE_IDLE_POLLS:-3}
SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS=${SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS:-10}
SCFUZZBENCH_QUEUE_BOOTSTRAP_WAIT_SECONDS=${SCFUZZBENCH_QUEUE_BOOTSTRAP_WAIT_SECONDS:-1800}
SCFUZZBENCH_SHARD_MAX_ATTEMPTS=${SCFUZZBENCH_SHARD_MAX_ATTEMPTS:-5}
SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS=${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS:-30}
SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS:-300}
SCFUZZBENCH_RUNNING_STALE_SECONDS=${SCFUZZBENCH_RUNNING_STALE_SECONDS:-900}
SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS=${SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS:-60}
SCFUZZBENCH_CLAIM_SETTLE_SECONDS=${SCFUZZBENCH_CLAIM_SETTLE_SECONDS:-2}
SCFUZZBENCH_CLAIM_STALE_SECONDS=${SCFUZZBENCH_CLAIM_STALE_SECONDS:-1200}
SCFUZZBENCH_LOCK_LEASE_SECONDS=${SCFUZZBENCH_LOCK_LEASE_SECONDS:-7200}
SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS=${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS:-120}
SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES=${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES:-3}

for var_name in \
  SCFUZZBENCH_QUEUE_IDLE_POLLS \
  SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS \
  SCFUZZBENCH_QUEUE_BOOTSTRAP_WAIT_SECONDS \
  SCFUZZBENCH_SHARD_MAX_ATTEMPTS \
  SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS \
  SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS \
  SCFUZZBENCH_RUNNING_STALE_SECONDS \
  SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS \
  SCFUZZBENCH_CLAIM_SETTLE_SECONDS \
  SCFUZZBENCH_CLAIM_STALE_SECONDS \
  SCFUZZBENCH_LOCK_LEASE_SECONDS \
  SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS \
  SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES; do
  value=${!var_name:-}
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    log "Invalid ${var_name}='${value}'"
    exit 1
  fi
done

if (( SCFUZZBENCH_QUEUE_IDLE_POLLS < 1 )); then
  SCFUZZBENCH_QUEUE_IDLE_POLLS=1
fi
if (( SCFUZZBENCH_SHARD_MAX_ATTEMPTS < 1 )); then
  SCFUZZBENCH_SHARD_MAX_ATTEMPTS=1
fi
if (( SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS < 1 )); then
  SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS=1
fi
if (( SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS < SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS )); then
  SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS=${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS}
fi
if (( SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS < 10 )); then
  SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS=10
fi
if (( SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS < 10 )); then
  SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS=10
fi
if (( SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES < 1 )); then
  SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES=1
fi

RUN_PREFIX="runs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}"
SHARD_PREFIX="${RUN_PREFIX}/queue/shards"
CLAIM_PREFIX="${RUN_PREFIX}/queue/claims"
STATUS_PREFIX="${RUN_PREFIX}/status"
EVENT_PREFIX="${STATUS_PREFIX}/events"
DLQ_PREFIX="${RUN_PREFIX}/dlq"
RUN_STATUS_KEY="${STATUS_PREFIX}/run.json"
LOCK_FAILURE_MARKER="${SCFUZZBENCH_ROOT}/lock-heartbeat.failed"
INSTANCE_ID_SAFE="unknown"

prepare_workspace
cache_instance_id || true
INSTANCE_ID_SAFE=$(echo "${SCFUZZBENCH_INSTANCE_ID:-unknown}" | tr -cs 'A-Za-z0-9._-' '-')
INSTANCE_ID_SAFE=${INSTANCE_ID_SAFE#-}
INSTANCE_ID_SAFE=${INSTANCE_ID_SAFE%-}
if [[ -z "${INSTANCE_ID_SAFE}" ]]; then
  INSTANCE_ID_SAFE="unknown"
fi

# Queue workers need aws + jq before they can poll/claim shards.
install_base_packages

iso_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

epoch_now() {
  date +%s
}

epoch_ms_now() {
  date +%s%3N
}

safe_token() {
  local raw=${1:-}
  local token
  token=$(echo "${raw}" | tr -cs 'A-Za-z0-9._-' '-')
  token=${token#-}
  token=${token%-}
  if [[ -z "${token}" ]]; then
    token="unknown"
  fi
  echo "${token}"
}

json_get_string() {
  local json=$1
  local query=$2
  jq -r "${query} // empty" <<<"${json}" 2>/dev/null || true
}

iso_to_epoch() {
  local raw=${1:-}
  if [[ -z "${raw}" ]]; then
    echo 0
    return 0
  fi
  python3 - "${raw}" <<'PY'
import datetime
import sys
raw = sys.argv[1]
try:
    dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    print(int(dt.timestamp()))
except Exception:
    print(0)
PY
}

backoff_seconds() {
  local attempt=$1
  local delay=$((SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS * (2 ** (attempt - 1))))
  if (( delay > SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS )); then
    delay=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}
  fi
  local jitter=$((RANDOM % (SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS + 1)))
  delay=$((delay + jitter))
  if (( delay > SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS )); then
    delay=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}
  fi
  echo "${delay}"
}

s3_put_json() {
  local key=$1
  local payload=$2
  local tmp
  tmp=$(mktemp)
  printf '%s' "${payload}" >"${tmp}"
  local rc=0
  retry_cmd 5 5 aws_cli s3api put-object \
    --bucket "${SCFUZZBENCH_S3_BUCKET}" \
    --key "${key}" \
    --content-type application/json \
    --body "${tmp}" \
    >/dev/null || rc=$?
  rm -f "${tmp}"
  return "${rc}"
}

s3_get_json() {
  local key=$1
  local out_file=$2
  local meta_file
  local err_file
  meta_file=$(mktemp)
  err_file=$(mktemp)

  if aws_cli s3api get-object \
    --bucket "${SCFUZZBENCH_S3_BUCKET}" \
    --key "${key}" \
    "${out_file}" \
    --output json >"${meta_file}" 2>"${err_file}"; then
    rm -f "${meta_file}" "${err_file}"
    return 0
  fi

  local err
  err=$(cat "${err_file}")
  rm -f "${meta_file}" "${err_file}"
  if grep -Eq "NoSuchKey|Not Found|status code: 404" <<<"${err}"; then
    return 1
  fi
  log "Failed to read s3://${SCFUZZBENCH_S3_BUCKET}/${key}: ${err}"
  return 2
}

s3_delete_key() {
  local key=$1
  aws_cli s3api delete-object --bucket "${SCFUZZBENCH_S3_BUCKET}" --key "${key}" >/dev/null 2>&1 || true
}

s3_list_keys() {
  local prefix=$1
  local token=""
  while true; do
    local payload
    if [[ -n "${token}" ]]; then
      payload=$(aws_cli s3api list-objects-v2 \
        --bucket "${SCFUZZBENCH_S3_BUCKET}" \
        --prefix "${prefix}" \
        --continuation-token "${token}" \
        --output json 2>/dev/null || true)
    else
      payload=$(aws_cli s3api list-objects-v2 \
        --bucket "${SCFUZZBENCH_S3_BUCKET}" \
        --prefix "${prefix}" \
        --output json 2>/dev/null || true)
    fi

    if [[ -z "${payload}" ]]; then
      return 0
    fi

    jq -r '.Contents[]?.Key' <<<"${payload}"

    local truncated
    truncated=$(jq -r '.IsTruncated // false' <<<"${payload}")
    if [[ "${truncated}" != "true" ]]; then
      break
    fi
    token=$(jq -r '.NextContinuationToken // empty' <<<"${payload}")
    if [[ -z "${token}" ]]; then
      break
    fi
  done
}

write_event() {
  local shard_key=$1
  local status=$2
  local attempt=${3:-0}
  local message=${4:-}
  local ts_ms
  ts_ms=$(epoch_ms_now)
  local safe_shard
  safe_shard=$(safe_token "${shard_key}")
  local safe_status
  safe_status=$(safe_token "${status}")
  local key="${EVENT_PREFIX}/${ts_ms}-${INSTANCE_ID_SAFE}-${safe_shard}-${safe_status}.json"
  local payload
  payload=$(jq -cn \
    --arg ts "$(iso_now)" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg shard_key "${shard_key}" \
    --arg status "${status}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg message "${message}" \
    --argjson attempt "${attempt}" \
    --argjson ts_epoch_ms "${ts_ms}" \
    '{
      ts: $ts,
      ts_epoch_ms: $ts_epoch_ms,
      run_id: $run_id,
      benchmark_uuid: $benchmark_uuid,
      shard_key: $shard_key,
      status: $status,
      attempt: $attempt,
      instance_id: $instance_id,
      message: $message
    }')
  s3_put_json "${key}" "${payload}" || true
}

write_worker_status() {
  local state=$1
  local shard_key=${2:-}
  local attempt=${3:-0}
  local message=${4:-}
  local key="${STATUS_PREFIX}/workers/${SCFUZZBENCH_INSTANCE_ID:-unknown}.json"
  local payload
  payload=$(jq -cn \
    --arg ts "$(iso_now)" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg state "${state}" \
    --arg shard_key "${shard_key}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg message "${message}" \
    --argjson attempt "${attempt}" \
    '{
      ts: $ts,
      run_id: $run_id,
      benchmark_uuid: $benchmark_uuid,
      state: $state,
      shard_key: $shard_key,
      attempt: $attempt,
      instance_id: $instance_id,
      message: $message
    }')
  s3_put_json "${key}" "${payload}" || true
}

read_run_status_json() {
  local tmp
  tmp=$(mktemp)
  if ! s3_get_json "${RUN_STATUS_KEY}" "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  cat "${tmp}"
  rm -f "${tmp}"
}

refresh_run_status() {
  local keys
  keys=$(s3_list_keys "${SHARD_PREFIX}/" | sort)
  local queued=0
  local running=0
  local retrying=0
  local succeeded=0
  local failed=0
  local timed_out=0
  local unknown=0

  if [[ -n "${keys}" ]]; then
    while IFS= read -r key; do
      [[ -z "${key}" ]] && continue
      local tmp
      tmp=$(mktemp)
      if ! s3_get_json "${key}" "${tmp}"; then
        rm -f "${tmp}"
        continue
      fi
      local status
      status=$(jq -r '.status // empty' "${tmp}")
      rm -f "${tmp}"
      case "${status}" in
        queued) queued=$((queued + 1)) ;;
        running) running=$((running + 1)) ;;
        retrying) retrying=$((retrying + 1)) ;;
        succeeded) succeeded=$((succeeded + 1)) ;;
        failed) failed=$((failed + 1)) ;;
        timed_out) timed_out=$((timed_out + 1)) ;;
        *) unknown=$((unknown + 1)) ;;
      esac
    done <<<"${keys}"
  fi

  local requested=$((queued + running + retrying + succeeded + failed + timed_out + unknown))
  local status="running"
  if (( queued == 0 && running == 0 && retrying == 0 )); then
    if (( failed > 0 || timed_out > 0 )); then
      status="failed"
    else
      status="completed"
    fi
  fi

  local old_status=""
  local existing
  if existing=$(read_run_status_json 2>/dev/null); then
    old_status=$(json_get_string "${existing}" '.status')
  fi

  local payload
  payload=$(jq -cn \
    --arg ts "$(iso_now)" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg status "${status}" \
    --arg queue_backend "s3" \
    --argjson requested "${requested}" \
    --argjson max_parallel "${SCFUZZBENCH_MAX_PARALLEL_EFFECTIVE:-1}" \
    --argjson shard_max_attempts "${SCFUZZBENCH_SHARD_MAX_ATTEMPTS}" \
    --argjson queued "${queued}" \
    --argjson running "${running}" \
    --argjson retrying "${retrying}" \
    --argjson succeeded "${succeeded}" \
    --argjson failed "${failed}" \
    --argjson timed_out "${timed_out}" \
    --argjson unknown "${unknown}" \
    --argjson updated_epoch "$(epoch_now)" \
    '{
      ts: $ts,
      run_id: $run_id,
      benchmark_uuid: $benchmark_uuid,
      queue_backend: $queue_backend,
      status: $status,
      requested_shards: $requested,
      max_parallel_instances: $max_parallel,
      shard_max_attempts: $shard_max_attempts,
      queued_count: $queued,
      running_count: $running,
      retrying_count: $retrying,
      succeeded_count: $succeeded,
      failed_count: $failed,
      timed_out_count: $timed_out,
      unknown_count: $unknown,
      updated_epoch: $updated_epoch,
      updated_at: $ts
    }')

  if [[ "${status}" == "completed" || "${status}" == "failed" ]]; then
    payload=$(jq -c '. + {completed_at: .updated_at}' <<<"${payload}")
  fi

  s3_put_json "${RUN_STATUS_KEY}" "${payload}" || true

  if [[ -n "${status}" && "${status}" != "${old_status}" ]]; then
    write_event "run" "${status}" 0 "run status transition"
  fi

  echo "${status}"
}

update_manifest_final_fields() {
  local status=$1
  local run_status_json=$2
  local tmp_manifest
  tmp_manifest=$(mktemp)
  local logs_manifest_key="logs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}/manifest.json"
  if ! s3_get_json "${logs_manifest_key}" "${tmp_manifest}"; then
    rm -f "${tmp_manifest}"
    return 0
  fi

  local merged
  merged=$(jq -c \
    --arg status "${status}" \
    --arg completed_at "$(iso_now)" \
    --argjson requested "$(jq -r '.requested_shards // 0' <<<"${run_status_json}")" \
    --argjson succeeded "$(jq -r '.succeeded_count // 0' <<<"${run_status_json}")" \
    --argjson failed "$(jq -r '(.failed_count // 0) + (.timed_out_count // 0)' <<<"${run_status_json}")" \
    '. + {
      final_status: $status,
      final_requested_shards: $requested,
      final_succeeded_shards: $succeeded,
      final_failed_shards: $failed,
      completed_at: $completed_at
    }' "${tmp_manifest}")
  rm -f "${tmp_manifest}"

  local out
  out=$(mktemp)
  printf '%s' "${merged}" >"${out}"
  retry_cmd 5 5 aws_cli s3 cp "${out}" "s3://${SCFUZZBENCH_S3_BUCKET}/${logs_manifest_key}" --no-progress >/dev/null || true
  retry_cmd 5 5 aws_cli s3 cp "${out}" "s3://${SCFUZZBENCH_S3_BUCKET}/runs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}/manifest.json" --no-progress >/dev/null || true
  rm -f "${out}"
}

release_lock_if_owner() {
  local lock_tmp
  lock_tmp=$(mktemp)
  if ! s3_get_json "${SCFUZZBENCH_LOCK_OBJECT_KEY}" "${lock_tmp}"; then
    rm -f "${lock_tmp}"
    return 0
  fi
  local owner
  owner=$(jq -r '.owner_run_id // empty' "${lock_tmp}")
  rm -f "${lock_tmp}"
  if [[ "${owner}" == "${SCFUZZBENCH_RUN_ID}" ]]; then
    s3_delete_key "${SCFUZZBENCH_LOCK_OBJECT_KEY}"
  fi
}

force_run_failed() {
  local reason=$1
  local current_status
  current_status=$(refresh_run_status)
  local run_json
  if ! run_json=$(read_run_status_json); then
    return 0
  fi
  local failed_payload
  failed_payload=$(jq -c --arg reason "${reason}" --arg completed_at "$(iso_now)" '. + {status:"failed", failure_reason:$reason, completed_at:$completed_at}' <<<"${run_json}")
  s3_put_json "${RUN_STATUS_KEY}" "${failed_payload}" || true
  write_event "run" "failed" 0 "${reason}"
  update_manifest_final_fields "failed" "${failed_payload}"
  if [[ "${current_status}" != "failed" ]]; then
    release_lock_if_owner || true
  fi
}

renew_lock_once() {
  local tmp
  tmp=$(mktemp)
  if ! s3_get_json "${SCFUZZBENCH_LOCK_OBJECT_KEY}" "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  local owner
  owner=$(jq -r '.owner_run_id // empty' "${tmp}")
  if [[ "${owner}" != "${SCFUZZBENCH_RUN_ID}" ]]; then
    rm -f "${tmp}"
    return 1
  fi
  local updated
  updated=$(jq -c \
    --arg now "$(iso_now)" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --argjson lease "${SCFUZZBENCH_LOCK_LEASE_SECONDS}" \
    --argjson now_epoch "$(epoch_now)" \
    '. + {
      updated_at: $now,
      heartbeat_instance_id: $instance_id,
      expires_at_epoch: ($now_epoch + $lease)
    }' "${tmp}")
  rm -f "${tmp}"
  s3_put_json "${SCFUZZBENCH_LOCK_OBJECT_KEY}" "${updated}"
}

start_lock_heartbeat() {
  (
    local consecutive_failures=0
    while true; do
      sleep "${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS}" || true
      if renew_lock_once; then
        consecutive_failures=0
      else
        consecutive_failures=$((consecutive_failures + 1))
        log "Lock heartbeat failed (${consecutive_failures}/${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES})"
        if (( consecutive_failures >= SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES )); then
          touch "${LOCK_FAILURE_MARKER}"
          break
        fi
      fi
    done
  ) &
  SCFUZZBENCH_LOCK_HEARTBEAT_PID=$!
}

stop_lock_heartbeat() {
  local pid=${SCFUZZBENCH_LOCK_HEARTBEAT_PID:-}
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
}

wait_for_run_status_bootstrap() {
  local waited=0
  while (( waited < SCFUZZBENCH_QUEUE_BOOTSTRAP_WAIT_SECONDS )); do
    local tmp
    tmp=$(mktemp)
    if s3_get_json "${RUN_STATUS_KEY}" "${tmp}"; then
      local status
      status=$(jq -r '.status // empty' "${tmp}")
      rm -f "${tmp}"
      if [[ "${status}" == "running" || "${status}" == "completed" || "${status}" == "failed" ]]; then
        return 0
      fi
    else
      rm -f "${tmp}"
    fi
    write_worker_status "waiting" "" 0 "waiting for queue bootstrap"
    sleep 10
    waited=$((waited + 10))
  done
  return 1
}

build_claim_payload() {
  local shard_key=$1
  local claim_key=$2
  jq -cn \
    --arg ts "$(iso_now)" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg shard_key "${shard_key}" \
    --arg claim_key "${claim_key}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    '{
      ts: $ts,
      run_id: $run_id,
      benchmark_uuid: $benchmark_uuid,
      shard_key: $shard_key,
      claim_key: $claim_key,
      instance_id: $instance_id
    }'
}

create_claim() {
  local shard_key=$1
  local ts_ms
  ts_ms=$(epoch_ms_now)
  local nonce
  nonce=$(printf '%08x' $((RANDOM * RANDOM + 1)))
  local claim_key="${CLAIM_PREFIX}/${shard_key}/${ts_ms}-${INSTANCE_ID_SAFE}-${nonce}.json"
  local payload
  payload=$(build_claim_payload "${shard_key}" "${claim_key}")
  s3_put_json "${claim_key}" "${payload}" || return 1
  echo "${claim_key}"
}

claim_epoch_from_key() {
  local key=$1
  local base
  base=$(basename "${key}")
  echo "${base}" | sed -E 's/^([0-9]+)-.*$/\1/'
}

cleanup_stale_claims() {
  local shard_key=$1
  local claim_keys
  claim_keys=$(s3_list_keys "${CLAIM_PREFIX}/${shard_key}/")
  if [[ -z "${claim_keys}" ]]; then
    return 0
  fi
  local now_ms
  now_ms=$(epoch_ms_now)
  while IFS= read -r key; do
    [[ -z "${key}" ]] && continue
    local claim_ms
    claim_ms=$(claim_epoch_from_key "${key}")
    if ! [[ "${claim_ms}" =~ ^[0-9]+$ ]]; then
      continue
    fi
    local age=$(((now_ms - claim_ms) / 1000))
    if (( age > SCFUZZBENCH_CLAIM_STALE_SECONDS )); then
      s3_delete_key "${key}"
    fi
  done <<<"${claim_keys}"
}

is_claim_leader() {
  local shard_key=$1
  local my_claim=$2
  cleanup_stale_claims "${shard_key}"
  local keys
  keys=$(s3_list_keys "${CLAIM_PREFIX}/${shard_key}/" | sort)
  if [[ -z "${keys}" ]]; then
    return 1
  fi
  local first
  first=$(head -n 1 <<<"${keys}")
  [[ "${first}" == "${my_claim}" ]]
}

set_shard_state() {
  local key=$1
  local payload=$2
  s3_put_json "${key}" "${payload}"
}

reclaim_stale_running_shard() {
  local shard_key=$1
  local key=$2
  local shard_json=$3

  local updated_at
  updated_at=$(json_get_string "${shard_json}" '.updated_at')
  local updated_epoch
  updated_epoch=$(iso_to_epoch "${updated_at}")
  local now
  now=$(epoch_now)
  local age=$((now - updated_epoch))
  if (( age < SCFUZZBENCH_RUNNING_STALE_SECONDS )); then
    return 0
  fi

  local attempt
  attempt=$(json_get_string "${shard_json}" '.attempt')
  if ! [[ "${attempt}" =~ ^[0-9]+$ ]]; then
    attempt=1
  fi

  if (( attempt >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
    local terminal
    terminal=$(jq -c \
      --arg now_iso "$(iso_now)" \
      --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
      '. + {
        status: "failed",
        last_error: "stale running shard reclaimed at max attempts",
        last_exit_code: 903,
        completed_at: $now_iso,
        updated_at: $now_iso,
        completed_by: $instance_id
      }' <<<"${shard_json}")
    set_shard_state "${key}" "${terminal}" || true
    write_event "${shard_key}" "failed" "${attempt}" "stale running shard terminalized"
    local dlq_payload
    dlq_payload=$(jq -c '. + {dlq_reason:"stale_running_max_attempts"}' <<<"${terminal}")
    s3_put_json "${DLQ_PREFIX}/${shard_key}-${attempt}.json" "${dlq_payload}" || true
    return 0
  fi

  local delay
  delay=$(backoff_seconds "${attempt}")
  local next_epoch=$((now + delay))
  local retrying
  retrying=$(jq -c \
    --arg now_iso "$(iso_now)" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg next_iso "$(date -u -d "@${next_epoch}" +"%Y-%m-%dT%H:%M:%SZ")" \
    --argjson delay "${delay}" \
    '. + {
      status: "retrying",
      last_error: ("stale running shard reclaimed; retry in " + ($delay|tostring) + "s"),
      last_exit_code: 903,
      updated_at: $now_iso,
      next_attempt_at: $next_iso,
      reclaimed_by: $instance_id
    }' <<<"${shard_json}")
  set_shard_state "${key}" "${retrying}" || true
  write_event "${shard_key}" "retrying" "${attempt}" "reclaimed stale running shard"
}

shard_ready_for_claim() {
  local shard_json=$1
  local status
  status=$(json_get_string "${shard_json}" '.status' | tr '[:upper:]' '[:lower:]')
  if [[ "${status}" == "queued" ]]; then
    return 0
  fi
  if [[ "${status}" != "retrying" ]]; then
    return 1
  fi
  local next_attempt_at
  next_attempt_at=$(json_get_string "${shard_json}" '.next_attempt_at')
  local next_epoch
  next_epoch=$(iso_to_epoch "${next_attempt_at}")
  local now
  now=$(epoch_now)
  (( now >= next_epoch ))
}

CLAIMED_SHARD_KEY=""
CLAIMED_STATE_KEY=""
CLAIMED_FUZZER_KEY=""
CLAIMED_RUN_INDEX=""
CLAIMED_ATTEMPT=""
CLAIMED_CLAIM_KEY=""

claim_shard() {
  local key=$1
  local shard_key
  shard_key=$(basename "${key}" .json)

  local tmp
  tmp=$(mktemp)
  if ! s3_get_json "${key}" "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  local shard_json
  shard_json=$(cat "${tmp}")
  rm -f "${tmp}"

  local status
  status=$(json_get_string "${shard_json}" '.status' | tr '[:upper:]' '[:lower:]')
  if [[ "${status}" == "running" ]]; then
    reclaim_stale_running_shard "${shard_key}" "${key}" "${shard_json}"
    return 1
  fi

  if ! shard_ready_for_claim "${shard_json}"; then
    return 1
  fi

  local claim_key
  claim_key=$(create_claim "${shard_key}") || return 1

  sleep "${SCFUZZBENCH_CLAIM_SETTLE_SECONDS}"
  if ! is_claim_leader "${shard_key}" "${claim_key}"; then
    s3_delete_key "${claim_key}"
    return 1
  fi

  tmp=$(mktemp)
  if ! s3_get_json "${key}" "${tmp}"; then
    rm -f "${tmp}"
    s3_delete_key "${claim_key}"
    return 1
  fi
  shard_json=$(cat "${tmp}")
  rm -f "${tmp}"

  if ! shard_ready_for_claim "${shard_json}"; then
    s3_delete_key "${claim_key}"
    return 1
  fi

  local attempt
  attempt=$(json_get_string "${shard_json}" '.attempt')
  if ! [[ "${attempt}" =~ ^[0-9]+$ ]]; then
    attempt=0
  fi
  attempt=$((attempt + 1))

  local claimed
  claimed=$(jq -c \
    --arg now_iso "$(iso_now)" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg claim_key "${claim_key}" \
    --argjson attempt "${attempt}" \
    '. + {
      status: "running",
      attempt: $attempt,
      claimed_by: $instance_id,
      claim_key: $claim_key,
      started_at: $now_iso,
      updated_at: $now_iso,
      next_attempt_at: $now_iso
    }' <<<"${shard_json}")

  set_shard_state "${key}" "${claimed}" || {
    s3_delete_key "${claim_key}"
    return 1
  }

  CLAIMED_SHARD_KEY=${shard_key}
  CLAIMED_STATE_KEY=${key}
  CLAIMED_FUZZER_KEY=$(json_get_string "${claimed}" '.fuzzer_key')
  CLAIMED_RUN_INDEX=$(json_get_string "${claimed}" '.run_index')
  CLAIMED_ATTEMPT=${attempt}
  CLAIMED_CLAIM_KEY=${claim_key}

  if [[ -z "${CLAIMED_FUZZER_KEY}" || ! "${CLAIMED_RUN_INDEX}" =~ ^[0-9]+$ ]]; then
    s3_delete_key "${claim_key}"
    return 1
  fi

  write_event "${CLAIMED_SHARD_KEY}" "running" "${CLAIMED_ATTEMPT}" "claimed shard"
  return 0
}

claim_next_shard() {
  CLAIMED_SHARD_KEY=""
  CLAIMED_STATE_KEY=""
  CLAIMED_FUZZER_KEY=""
  CLAIMED_RUN_INDEX=""
  CLAIMED_ATTEMPT=""
  CLAIMED_CLAIM_KEY=""

  local keys
  keys=$(s3_list_keys "${SHARD_PREFIX}/")
  if [[ -z "${keys}" ]]; then
    return 1
  fi

  while IFS= read -r key; do
    [[ -z "${key}" ]] && continue
    if claim_shard "${key}"; then
      return 0
    fi
  done <<<"$(sort <<<"${keys}")"

  return 1
}

heartbeat_running_shard() {
  local state_key=$1
  local attempt=$2
  while true; do
    sleep "${SCFUZZBENCH_SHARD_HEARTBEAT_SECONDS}" || true
    local tmp
    tmp=$(mktemp)
    if ! s3_get_json "${state_key}" "${tmp}"; then
      rm -f "${tmp}"
      break
    fi
    local status
    status=$(jq -r '.status // empty' "${tmp}")
    local current_attempt
    current_attempt=$(jq -r '.attempt // 0' "${tmp}")
    if [[ "${status}" != "running" || "${current_attempt}" != "${attempt}" ]]; then
      rm -f "${tmp}"
      break
    fi

    local updated
    updated=$(jq -c --arg now_iso "$(iso_now)" --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" '. + {updated_at:$now_iso, heartbeat_instance_id:$instance_id}' "${tmp}")
    rm -f "${tmp}"
    set_shard_state "${state_key}" "${updated}" || true
  done
}

write_dlq_entry() {
  local shard_key=$1
  local attempt=$2
  local payload=$3
  s3_put_json "${DLQ_PREFIX}/${shard_key}-${attempt}.json" "${payload}" || true
}

finalize_claimed_shard() {
  local result_status=$1
  local exit_code=$2
  local message=$3

  local tmp
  tmp=$(mktemp)
  if ! s3_get_json "${CLAIMED_STATE_KEY}" "${tmp}"; then
    rm -f "${tmp}"
    s3_delete_key "${CLAIMED_CLAIM_KEY}"
    return 1
  fi
  local shard_json
  shard_json=$(cat "${tmp}")
  rm -f "${tmp}"

  local payload
  payload=$(jq -c \
    --arg status "${result_status}" \
    --arg now_iso "$(iso_now)" \
    --arg msg "${message}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --argjson exit_code "${exit_code}" \
    '. + {
      status: $status,
      updated_at: $now_iso,
      last_error: $msg,
      last_exit_code: $exit_code,
      completed_by: $instance_id
    }' <<<"${shard_json}")

  if [[ "${result_status}" == "succeeded" || "${result_status}" == "failed" || "${result_status}" == "timed_out" ]]; then
    payload=$(jq -c '. + {completed_at: .updated_at}' <<<"${payload}")
  fi

  set_shard_state "${CLAIMED_STATE_KEY}" "${payload}" || true
  write_event "${CLAIMED_SHARD_KEY}" "${result_status}" "${CLAIMED_ATTEMPT}" "${message}"

  if [[ "${result_status}" == "failed" || "${result_status}" == "timed_out" ]]; then
    write_dlq_entry "${CLAIMED_SHARD_KEY}" "${CLAIMED_ATTEMPT}" "${payload}"
  fi

  s3_delete_key "${CLAIMED_CLAIM_KEY}"
}

retry_claimed_shard() {
  local exit_code=$1
  local message=$2

  local delay
  delay=$(backoff_seconds "${CLAIMED_ATTEMPT}")
  local next_epoch=$(( $(epoch_now) + delay ))
  local next_iso
  next_iso=$(date -u -d "@${next_epoch}" +"%Y-%m-%dT%H:%M:%SZ")

  local tmp
  tmp=$(mktemp)
  if ! s3_get_json "${CLAIMED_STATE_KEY}" "${tmp}"; then
    rm -f "${tmp}"
    s3_delete_key "${CLAIMED_CLAIM_KEY}"
    return 1
  fi
  local shard_json
  shard_json=$(cat "${tmp}")
  rm -f "${tmp}"

  local payload
  payload=$(jq -c \
    --arg now_iso "$(iso_now)" \
    --arg next_iso "${next_iso}" \
    --arg msg "${message}" \
    --argjson delay "${delay}" \
    --argjson exit_code "${exit_code}" \
    '. + {
      status: "retrying",
      updated_at: $now_iso,
      next_attempt_at: $next_iso,
      last_error: ($msg + "; retry in " + ($delay|tostring) + "s"),
      last_exit_code: $exit_code
    }' <<<"${shard_json}")

  set_shard_state "${CLAIMED_STATE_KEY}" "${payload}" || true
  write_event "${CLAIMED_SHARD_KEY}" "retrying" "${CLAIMED_ATTEMPT}" "${message}"
  s3_delete_key "${CLAIMED_CLAIM_KEY}"
}

ensure_fuzzer_installed() {
  local fuzzer_key=$1
  local marker="/opt/scfuzzbench/.installed-${fuzzer_key}"
  if [[ -f "${marker}" ]]; then
    return 0
  fi

  local install_script="/opt/scfuzzbench/fuzzers/${fuzzer_key}/install.sh"
  if [[ ! -x "${install_script}" ]]; then
    log "Missing install script: ${install_script}"
    return 1
  fi

  log "Installing fuzzer '${fuzzer_key}'"
  if ! bash "${install_script}"; then
    log "Fuzzer install failed: ${fuzzer_key}"
    return 1
  fi

  touch "${marker}"
  return 0
}

run_claimed_shard() {
  write_worker_status "running" "${CLAIMED_SHARD_KEY}" "${CLAIMED_ATTEMPT}" "processing shard"

  local install_rc=0
  ensure_fuzzer_installed "${CLAIMED_FUZZER_KEY}" || install_rc=$?
  if (( install_rc != 0 )); then
    if (( CLAIMED_ATTEMPT >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
      finalize_claimed_shard "failed" "${install_rc}" "install failed at max attempts"
    else
      retry_claimed_shard "${install_rc}" "install failed"
    fi
    return
  fi

  heartbeat_running_shard "${CLAIMED_STATE_KEY}" "${CLAIMED_ATTEMPT}" &
  local hb_pid=$!

  local shard_root="/opt/scfuzzbench/shards/${CLAIMED_SHARD_KEY}-a${CLAIMED_ATTEMPT}"
  mkdir -p "${shard_root}/work" "${shard_root}/logs" "${shard_root}/corpus"

  local artifact_suffix="${CLAIMED_SHARD_KEY}-a${CLAIMED_ATTEMPT}"
  local run_rc=0
  set +e
  SCFUZZBENCH_QUEUE_MODE=1 \
    SCFUZZBENCH_ARTIFACT_SUFFIX="${artifact_suffix}" \
    SCFUZZBENCH_WORKDIR="${shard_root}/work" \
    SCFUZZBENCH_LOG_DIR="${shard_root}/logs" \
    SCFUZZBENCH_CORPUS_DIR="${shard_root}/corpus" \
    bash "/opt/scfuzzbench/fuzzers/${CLAIMED_FUZZER_KEY}/run.sh"
  run_rc=$?
  set -e

  kill "${hb_pid}" >/dev/null 2>&1 || true
  wait "${hb_pid}" >/dev/null 2>&1 || true

  if (( run_rc == 0 )); then
    finalize_claimed_shard "succeeded" 0 "shard succeeded"
    return
  fi

  local terminal_status="failed"
  if (( run_rc == 124 || run_rc == 137 )); then
    terminal_status="timed_out"
  fi

  if (( CLAIMED_ATTEMPT >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
    finalize_claimed_shard "${terminal_status}" "${run_rc}" "terminal at max attempts"
    return
  fi

  retry_claimed_shard "${run_rc}" "non-terminal failure"
}

main() {
  log "Starting S3 queue worker for run ${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}"

  if ! wait_for_run_status_bootstrap; then
    log "Queue bootstrap status did not appear in time"
    write_worker_status "failed" "" 0 "missing bootstrap status"
    exit 1
  fi

  start_lock_heartbeat

  local idle_polls=0
  while true; do
    if [[ -f "${LOCK_FAILURE_MARKER}" ]]; then
      log "Lock heartbeat failed repeatedly; fail-closed"
      force_run_failed "lock heartbeat failures"
      write_worker_status "failed" "" 0 "lock heartbeat failures"
      stop_lock_heartbeat
      exit 1
    fi

    if claim_next_shard; then
      idle_polls=0
      run_claimed_shard
      local status_after
      status_after=$(refresh_run_status)
      if [[ "${status_after}" == "completed" || "${status_after}" == "failed" ]]; then
        local run_json
        if run_json=$(read_run_status_json 2>/dev/null); then
          update_manifest_final_fields "${status_after}" "${run_json}"
        fi
        release_lock_if_owner || true
      fi
      continue
    fi

    idle_polls=$((idle_polls + 1))
    write_worker_status "idle" "" 0 "idle poll ${idle_polls}"

    local run_status
    run_status=$(refresh_run_status)
    if [[ "${run_status}" == "completed" || "${run_status}" == "failed" ]]; then
      local run_json
      if run_json=$(read_run_status_json 2>/dev/null); then
        update_manifest_final_fields "${run_status}" "${run_json}"
      fi
      release_lock_if_owner || true
      break
    fi

    if (( idle_polls >= SCFUZZBENCH_QUEUE_IDLE_POLLS )); then
      local final_status
      final_status=$(refresh_run_status)
      if [[ "${final_status}" == "completed" || "${final_status}" == "failed" ]]; then
        local run_json
        if run_json=$(read_run_status_json 2>/dev/null); then
          update_manifest_final_fields "${final_status}" "${run_json}"
        fi
        release_lock_if_owner || true
        break
      fi
      idle_polls=0
    fi

    sleep "${SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS}"
  done

  stop_lock_heartbeat
  write_worker_status "completed" "" 0 "worker exiting"
  log "Queue worker completed"
}

main "$@"
