#!/usr/bin/env bash
set -euo pipefail

source /opt/scfuzzbench/common.sh

require_env \
  SCFUZZBENCH_QUEUE_URL \
  SCFUZZBENCH_RUN_STATE_TABLE \
  SCFUZZBENCH_LOCK_TABLE \
  SCFUZZBENCH_LOCK_NAME \
  SCFUZZBENCH_RUN_ID \
  SCFUZZBENCH_BENCHMARK_UUID \
  SCFUZZBENCH_S3_BUCKET

SCFUZZBENCH_QUEUE_WAIT_SECONDS=${SCFUZZBENCH_QUEUE_WAIT_SECONDS:-20}
SCFUZZBENCH_QUEUE_IDLE_POLLS=${SCFUZZBENCH_QUEUE_IDLE_POLLS:-3}
SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS=${SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS:-10}
SCFUZZBENCH_SHARD_MAX_ATTEMPTS=${SCFUZZBENCH_SHARD_MAX_ATTEMPTS:-5}
SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS=${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS:-30}
SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS:-300}
SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS=${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS:-600}
SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS=${SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS:-300}
SCFUZZBENCH_RUNNING_STALE_SECONDS=${SCFUZZBENCH_RUNNING_STALE_SECONDS:-0}
SCFUZZBENCH_LOCK_LEASE_SECONDS=${SCFUZZBENCH_LOCK_LEASE_SECONDS:-7200}
SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS=${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS:-120}
SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES=${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES:-3}

if ! [[ "${SCFUZZBENCH_QUEUE_WAIT_SECONDS}" =~ ^[0-9]+$ ]]; then
  SCFUZZBENCH_QUEUE_WAIT_SECONDS=20
fi
if ! [[ "${SCFUZZBENCH_QUEUE_IDLE_POLLS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_QUEUE_IDLE_POLLS}" -lt 1 ]]; then
  SCFUZZBENCH_QUEUE_IDLE_POLLS=3
fi
if ! [[ "${SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS}" =~ ^[0-9]+$ ]]; then
  SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS=10
fi
if ! [[ "${SCFUZZBENCH_SHARD_MAX_ATTEMPTS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_SHARD_MAX_ATTEMPTS}" -lt 1 ]]; then
  SCFUZZBENCH_SHARD_MAX_ATTEMPTS=5
fi
if ! [[ "${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS}" -lt 1 ]]; then
  SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS=30
fi
if ! [[ "${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}" -lt "${SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS}" ]]; then
  SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS=300
fi
if ! [[ "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" -lt 60 ]]; then
  SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS=600
fi
if ! [[ "${SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS}" -lt 30 ]]; then
  SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS=300
fi
if ! [[ "${SCFUZZBENCH_RUNNING_STALE_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_RUNNING_STALE_SECONDS}" -lt 60 ]]; then
  SCFUZZBENCH_RUNNING_STALE_SECONDS=$((SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS + (SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS * 2) + 60))
fi
if [[ "${SCFUZZBENCH_RUNNING_STALE_SECONDS}" -lt 300 ]]; then
  SCFUZZBENCH_RUNNING_STALE_SECONDS=300
fi
if ! [[ "${SCFUZZBENCH_LOCK_LEASE_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_LOCK_LEASE_SECONDS}" -lt 300 ]]; then
  SCFUZZBENCH_LOCK_LEASE_SECONDS=7200
fi
if ! [[ "${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS}" -lt 30 ]]; then
  SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS=120
fi
if ! [[ "${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES}" =~ ^[0-9]+$ ]] || [[ "${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES}" -lt 1 ]]; then
  SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES=3
fi

RUN_PK="RUN#${SCFUZZBENCH_RUN_ID}#${SCFUZZBENCH_BENCHMARK_UUID}"
STATUS_PREFIX="${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}"
LOCK_HEARTBEAT_FAILURE_MARKER="${SCFUZZBENCH_ROOT}/lock-heartbeat.failed"

prepare_workspace
cache_instance_id || true

meta_key_json() {
  jq -cn --arg pk "${RUN_PK}" '{pk:{S:$pk}, sk:{S:"META"}}'
}

shard_key_json() {
  local shard_key=$1
  jq -cn --arg pk "${RUN_PK}" --arg sk "SHARD#${shard_key}" '{pk:{S:$pk}, sk:{S:$sk}}'
}

get_run_meta() {
  aws_cli dynamodb get-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(meta_key_json)" \
    --consistent-read \
    --output json
}

run_meta_num() {
  local meta_json=$1
  local field=$2
  local fallback=$3
  local value
  value=$(jq -r ".Item.${field}.N // empty" <<<"${meta_json}")
  if [[ -z "${value}" || ! "${value}" =~ ^-?[0-9]+$ ]]; then
    echo "${fallback}"
  else
    echo "${value}"
  fi
}

run_meta_status() {
  local meta_json=$1
  jq -r '.Item.status.S // "unknown"' <<<"${meta_json}"
}

delete_message_safe() {
  local receipt_handle=$1
  aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
}

get_shard_status() {
  local shard_key=$1
  local item
  item=$(aws_cli dynamodb get-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(shard_key_json "${shard_key}")" \
    --consistent-read \
    --output json 2>/dev/null || true)
  jq -r '.Item.status.S // "missing"' <<<"${item}"
}

shard_updated_age_seconds() {
  local updated_at=$1
  if [[ -z "${updated_at}" ]]; then
    echo "${SCFUZZBENCH_RUNNING_STALE_SECONDS}"
    return 0
  fi
  local updated_epoch
  updated_epoch=$(python3 - "${updated_at}" <<'PY'
import datetime
import sys

raw = sys.argv[1]
try:
    dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    print(int(dt.timestamp()))
except Exception:
    print(0)
PY
)
  if ! [[ "${updated_epoch}" =~ ^[0-9]+$ ]] || (( updated_epoch <= 0 )); then
    echo "${SCFUZZBENCH_RUNNING_STALE_SECONDS}"
    return 0
  fi
  local now_epoch
  now_epoch=$(date +%s)
  local age=$((now_epoch - updated_epoch))
  if (( age < 0 )); then
    age=0
  fi
  echo "${age}"
}

retry_shard_transition() {
  local max_attempts=$1
  shift
  local fn_name=$1
  shift

  local attempt=1
  local rc=0
  while true; do
    rc=0
    "${fn_name}" "$@" || rc=$?
    if (( rc == 0 )); then
      return 0
    fi
    if (( rc == 1 )); then
      return 1
    fi
    if (( attempt >= max_attempts )); then
      return "${rc}"
    fi
    local delay=$((attempt * 2))
    if (( delay > 10 )); then
      delay=10
    fi
    sleep "${delay}" || true
    attempt=$((attempt + 1))
  done
}

handle_unclaimable_message() {
  local shard_key=$1
  local receipt_handle=$2
  local context=$3
  local fallback_fuzzer_key=${4:-}
  local fallback_run_index=${5:-}
  local shard_item
  shard_item=$(aws_cli dynamodb get-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(shard_key_json "${shard_key}")" \
    --consistent-read \
    --output json 2>/dev/null || true)
  local current_status
  local current_attempts
  local current_updated_at
  local current_fuzzer_key
  local current_run_index
  current_status=$(jq -r '.Item.status.S // "missing"' <<<"${shard_item}")
  current_attempts=$(jq -r '.Item.attempts.N // "0"' <<<"${shard_item}")
  current_updated_at=$(jq -r '.Item.updated_at.S // ""' <<<"${shard_item}")
  current_fuzzer_key=$(jq -r '.Item.fuzzer_key.S // empty' <<<"${shard_item}")
  current_run_index=$(jq -r '.Item.run_index.N // empty' <<<"${shard_item}")
  if [[ -z "${current_fuzzer_key}" ]]; then
    current_fuzzer_key="${fallback_fuzzer_key}"
  fi
  if [[ -z "${current_run_index}" ]]; then
    current_run_index="${fallback_run_index}"
  fi

  case "${current_status}" in
    succeeded|failed|timed_out)
      log "${context}: shard '${shard_key}' already terminal (${current_status}); deleting duplicate message."
      delete_message_safe "${receipt_handle}"
      mark_run_completed_if_possible || true
      ;;
    running)
      local age_seconds
      age_seconds=$(shard_updated_age_seconds "${current_updated_at}")
      if (( age_seconds >= SCFUZZBENCH_RUNNING_STALE_SECONDS )); then
        if ! [[ "${current_attempts}" =~ ^[0-9]+$ ]] || (( current_attempts < 1 )); then
          current_attempts=1
        fi
        if [[ -n "${current_fuzzer_key}" && "${current_run_index}" =~ ^[0-9]+$ ]]; then
          local reclaim_rc=0
          retry_shard_transition 4 mark_shard_retrying "${shard_key}" "${current_fuzzer_key}" "${current_run_index}" "${current_attempts}" "903" "stale_running_reclaim_age_${age_seconds}s" || reclaim_rc=$?
          if (( reclaim_rc == 0 )); then
            local reclaim_delay
            reclaim_delay=$(backoff_seconds "${current_attempts}")
            log "${context}: reclaimed stale running shard '${shard_key}' (age=${age_seconds}s); retrying in ${reclaim_delay}s."
            aws_cli sqs change-message-visibility \
              --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
              --receipt-handle "${receipt_handle}" \
              --visibility-timeout "${reclaim_delay}" \
              >/dev/null || true
            return
          fi
          if (( reclaim_rc == 1 )); then
            log "${context}: shard '${shard_key}' running reclaim raced with another transition; retrying shortly."
            aws_cli sqs change-message-visibility \
              --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
              --receipt-handle "${receipt_handle}" \
              --visibility-timeout 15 \
              >/dev/null || true
            return
          fi
        fi
      fi
      log "${context}: shard '${shard_key}' is running (age=${age_seconds}s); extending visibility."
      aws_cli sqs change-message-visibility \
        --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
        --receipt-handle "${receipt_handle}" \
        --visibility-timeout "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" \
        >/dev/null || true
      ;;
    queued|retrying|launching)
      log "${context}: shard '${shard_key}' currently ${current_status}; retrying message shortly."
      aws_cli sqs change-message-visibility \
        --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
        --receipt-handle "${receipt_handle}" \
        --visibility-timeout 15 \
        >/dev/null || true
      ;;
    *)
      log "${context}: shard '${shard_key}' status is '${current_status}'; keeping message for retry."
      aws_cli sqs change-message-visibility \
        --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
        --receipt-handle "${receipt_handle}" \
        --visibility-timeout 30 \
        >/dev/null || true
      ;;
  esac
}

claim_shard_for_processing() {
  local shard_key=$1
  local fuzzer_key=$2
  local run_index=$3
  local now
  now=$(date -Is)

  local names
  local values
  names=$(jq -cn '{"#status":"status"}')
  values=$(jq -cn \
    --arg queued "queued" \
    --arg retrying "retrying" \
    --arg launching "launching" \
    --arg running "running" \
    --arg fuzzer_key "${fuzzer_key}" \
    --arg run_index "${run_index}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg now "${now}" \
    --arg zero "0" \
    --arg one "1" \
    '{
      ":queued":{S:$queued},
      ":retrying":{S:$retrying},
      ":launching":{S:$launching},
      ":running":{S:$running},
      ":fuzzer_key":{S:$fuzzer_key},
      ":run_index":{N:$run_index},
      ":instance_id":{S:$instance_id},
      ":now":{S:$now},
      ":zero":{N:$zero},
      ":one":{N:$one}
    }')

  local output
  local rc
  set +e
  output=$(aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(shard_key_json "${shard_key}")" \
    --condition-expression "#status = :queued OR #status = :retrying OR #status = :launching" \
    --update-expression "SET #status = :running, fuzzer_key = :fuzzer_key, run_index = :run_index, attempts = if_not_exists(attempts, :zero) + :one, instance_id = :instance_id, updated_at = :now" \
    --expression-attribute-names "${names}" \
    --expression-attribute-values "${values}" \
    --return-values ALL_NEW \
    --output json 2>&1)
  rc=$?
  set -e
  if (( rc == 0 )); then
    local claimed_attempt
    claimed_attempt=$(jq -r '.Attributes.attempts.N // empty' <<<"${output}")
    if [[ "${claimed_attempt}" =~ ^[0-9]+$ ]] && (( claimed_attempt >= 1 )); then
      echo "${claimed_attempt}"
      return 0
    fi
    log "Shard '${shard_key}' claim succeeded but attempts counter missing in response."
    return 2
  fi
  if grep -Eq "ConditionalCheckFailedException|ConditionalCheckFailed" <<<"${output}"; then
    return 1
  fi
  log "Failed to claim shard '${shard_key}': ${output}"
  return 2
}

mark_shard_retrying() {
  local shard_key=$1
  local fuzzer_key=$2
  local run_index=$3
  local attempts=$4
  local exit_code=$5
  local error_message=$6
  local now
  now=$(date -Is)

  local names
  local values
  names=$(jq -cn '{"#status":"status"}')
  values=$(jq -cn \
    --arg running "running" \
    --arg retrying "retrying" \
    --arg fuzzer_key "${fuzzer_key}" \
    --arg run_index "${run_index}" \
    --arg attempts "${attempts}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg now "${now}" \
    --arg exit_code "${exit_code}" \
    --arg error_message "${error_message}" \
    '{
      ":running":{S:$running},
      ":retrying":{S:$retrying},
      ":fuzzer_key":{S:$fuzzer_key},
      ":run_index":{N:$run_index},
      ":attempts":{N:$attempts},
      ":instance_id":{S:$instance_id},
      ":now":{S:$now},
      ":exit_code":{N:$exit_code},
      ":error_message":{S:$error_message}
    }')

  local output
  local rc
  set +e
  output=$(aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(shard_key_json "${shard_key}")" \
    --condition-expression "#status = :running" \
    --update-expression "SET #status = :retrying, fuzzer_key = :fuzzer_key, run_index = :run_index, attempts = :attempts, instance_id = :instance_id, updated_at = :now, last_exit_code = :exit_code, last_error = :error_message" \
    --expression-attribute-names "${names}" \
    --expression-attribute-values "${values}" \
    --output json 2>&1)
  rc=$?
  set -e
  if (( rc == 0 )); then
    return 0
  fi
  if grep -q "ConditionalCheckFailedException" <<<"${output}"; then
    return 1
  fi
  log "Failed to transition shard '${shard_key}' to retrying: ${output}"
  return 2
}

complete_shard_and_increment_counter() {
  local shard_key=$1
  local fuzzer_key=$2
  local run_index=$3
  local attempts=$4
  local status=$5
  local exit_code=$6
  local error_message=$7
  local counter_field=$8
  local now
  now=$(date -Is)

  local tx_payload
  tx_payload=$(jq -cn \
    --arg table "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --arg run_pk "${RUN_PK}" \
    --arg shard_key "${shard_key}" \
    --arg status "${status}" \
    --arg running "running" \
    --arg fuzzer_key "${fuzzer_key}" \
    --arg run_index "${run_index}" \
    --arg attempts "${attempts}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg now "${now}" \
    --arg exit_code "${exit_code}" \
    --arg error_message "${error_message}" \
    --arg counter_field "${counter_field}" \
    '{
      TransactItems: [
        {
          Update: {
            TableName: $table,
            Key: {pk:{S:$run_pk}, sk:{S:("SHARD#" + $shard_key)}},
            ConditionExpression: "#status = :running",
            UpdateExpression: "SET #status = :status, fuzzer_key = :fuzzer_key, run_index = :run_index, attempts = :attempts, instance_id = :instance_id, updated_at = :now, last_exit_code = :exit_code, last_error = :error_message",
            ExpressionAttributeNames: {"#status":"status"},
            ExpressionAttributeValues: {
              ":running":{S:$running},
              ":status":{S:$status},
              ":fuzzer_key":{S:$fuzzer_key},
              ":run_index":{N:$run_index},
              ":attempts":{N:$attempts},
              ":instance_id":{S:$instance_id},
              ":now":{S:$now},
              ":exit_code":{N:$exit_code},
              ":error_message":{S:$error_message}
            }
          }
        },
        {
          Update: {
            TableName: $table,
            Key: {pk:{S:$run_pk}, sk:{S:"META"}},
            UpdateExpression: ("ADD " + $counter_field + " :one SET updated_at = :now"),
            ExpressionAttributeValues: {
              ":one":{N:"1"},
              ":now":{S:$now}
            }
          }
        }
      ]
    }')

  local output
  local rc
  set +e
  output=$(aws_cli dynamodb transact-write-items \
    --transact-items "${tx_payload}" \
    --output json 2>&1)
  rc=$?
  set -e
  if (( rc == 0 )); then
    return 0
  fi
  if grep -Eq "ConditionalCheckFailedException|ConditionalCheckFailed" <<<"${output}"; then
    return 1
  fi
  log "Failed to finalize shard '${shard_key}' with status '${status}': ${output}"
  return 2
}

set_run_status() {
  local new_status=$1
  local now
  now=$(date -Is)
  local values
  values=$(jq -cn \
    --arg running "running" \
    --arg status "${new_status}" \
    --arg now "${now}" \
    '{":running":{S:$running},":status":{S:$status},":now":{S:$now}}')
  local names
  names=$(jq -cn '{"#status":"status"}')

  local output
  local rc
  set +e
  output=$(aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(meta_key_json)" \
    --condition-expression "#status = :running" \
    --update-expression "SET #status = :status, completed_at = :now, updated_at = :now" \
    --expression-attribute-names "${names}" \
    --expression-attribute-values "${values}" \
    --output json 2>&1)
  rc=$?
  set -e
  if (( rc == 0 )); then
    return 0
  fi
  if grep -Eq "ConditionalCheckFailedException|ConditionalCheckFailed" <<<"${output}"; then
    return 1
  fi
  log "Failed to set run status '${new_status}': ${output}"
  return 2
}

mark_run_failed_due_to_lock_error() {
  local reason=${1:-lock_heartbeat_failed}
  local meta="{}"
  local meta_rc=0
  local status="unknown"

  set +e
  meta=$(get_run_meta 2>/dev/null)
  meta_rc=$?
  set -e
  if (( meta_rc == 0 )); then
    status=$(run_meta_status "${meta}")
  else
    log "Unable to read run metadata while handling ${reason}; proceeding with best-effort terminalization."
  fi

  if [[ "${status}" == "completed" || "${status}" == "failed" ]]; then
    return 0
  fi

  if ! upload_run_status_json "failed"; then
    log "Failed to publish failed status.json for reason '${reason}'."
  fi

  local set_rc=0
  set_run_status "failed" || set_rc=$?
  if (( set_rc > 1 )); then
    log "Failed to persist failed run-state status for reason '${reason}'."
    return 1
  fi

  if ! release_global_lock; then
    log "Run marked failed for reason '${reason}', but lock release failed; will rely on lease expiry/recovery."
  fi
  return 0
}

release_global_lock() {
  local values
  values=$(jq -cn --arg owner "${SCFUZZBENCH_RUN_ID}" '{":owner":{S:$owner}}')
  local key
  key=$(jq -cn --arg name "${SCFUZZBENCH_LOCK_NAME}" '{lock_name:{S:$name}}')
  local attempt=1
  local max_attempts=5
  local output=""
  local rc=0
  while (( attempt <= max_attempts )); do
    set +e
    output=$(aws_cli dynamodb delete-item \
      --table-name "${SCFUZZBENCH_LOCK_TABLE}" \
      --key "${key}" \
      --condition-expression "owner_run_id = :owner" \
      --expression-attribute-values "${values}" \
      --output json 2>&1)
    rc=$?
    set -e
    if (( rc == 0 )); then
      return 0
    fi
    if grep -Eq "ConditionalCheckFailedException|ConditionalCheckFailed" <<<"${output}"; then
      # Ownership changed or lock already removed; treat as released.
      return 0
    fi
    if (( attempt >= max_attempts )); then
      log "Failed to release global lock after ${max_attempts} attempts: ${output}"
      return 1
    fi
    local delay=$((attempt * 2))
    if (( delay > 10 )); then
      delay=10
    fi
    delay=$((delay + (RANDOM % 3)))
    log "Global lock release failed (attempt ${attempt}/${max_attempts}): ${output}; retrying in ${delay}s."
    sleep "${delay}" || true
    attempt=$((attempt + 1))
  done
  return 1
}

extend_global_lock_lease() {
  local now_epoch
  now_epoch=$(date +%s)
  local expires_at=$((now_epoch + SCFUZZBENCH_LOCK_LEASE_SECONDS))
  local values
  values=$(jq -cn \
    --arg owner "${SCFUZZBENCH_RUN_ID}" \
    --arg expires_at "${expires_at}" \
    --arg now "$(date -Is)" \
    '{":owner":{S:$owner},":expires_at":{N:$expires_at},":now":{S:$now}}')
  local key
  key=$(jq -cn --arg name "${SCFUZZBENCH_LOCK_NAME}" '{lock_name:{S:$name}}')
  aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_LOCK_TABLE}" \
    --key "${key}" \
    --condition-expression "owner_run_id = :owner" \
    --update-expression "SET expires_at = :expires_at, updated_at = :now" \
    --expression-attribute-values "${values}" \
    >/dev/null
}

heartbeat_lock_lease() {
  local failures=0
  while true; do
    sleep "${SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS}" || true
    if extend_global_lock_lease >/dev/null 2>&1; then
      failures=0
      continue
    fi
    failures=$((failures + 1))
    log "Global lock lease heartbeat failed (${failures}/${SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES})."
    if (( failures >= SCFUZZBENCH_LOCK_HEARTBEAT_MAX_FAILURES )); then
      log "Global lock lease heartbeat exceeded failure budget; failing closed."
      : >"${LOCK_HEARTBEAT_FAILURE_MARKER}"
      mark_run_failed_due_to_lock_error "lock_heartbeat_failure_budget_exhausted" || true
      kill -TERM "${WORKER_PARENT_PID}" >/dev/null 2>&1 || true
      return 1
    fi
  done
}

upload_final_manifest_json() {
  local status=$1
  local requested=$2
  local succeeded=$3
  local failed=$4
  local completed_at=$5
  local tmp_dir="${SCFUZZBENCH_ROOT}/queue-final"
  local base_manifest="${tmp_dir}/manifest-base.json"
  local final_manifest="${tmp_dir}/manifest-final.json"
  mkdir -p "${tmp_dir}"

  if ! retry_cmd 5 30 aws_cli s3 cp "s3://${SCFUZZBENCH_S3_BUCKET}/runs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}/manifest.json" "${base_manifest}" --no-progress; then
    if ! retry_cmd 5 30 aws_cli s3 cp "s3://${SCFUZZBENCH_S3_BUCKET}/logs/${STATUS_PREFIX}/manifest.json" "${base_manifest}" --no-progress; then
      log "Skipping final manifest update: unable to read base manifest from runs/ or logs/."
      return 1
    fi
  fi

  jq \
    --arg status "${status}" \
    --arg requested "${requested}" \
    --arg succeeded "${succeeded}" \
    --arg failed "${failed}" \
    --arg completed_at "${completed_at}" \
    '. + {
      final_status: $status,
      final_requested_shards: ($requested|tonumber),
      final_succeeded_shards: ($succeeded|tonumber),
      final_failed_shards: ($failed|tonumber),
      completed_at: $completed_at
    }' "${base_manifest}" >"${final_manifest}"

  retry_cmd 5 30 aws_cli s3 cp "${final_manifest}" "s3://${SCFUZZBENCH_S3_BUCKET}/logs/${STATUS_PREFIX}/manifest.json" --no-progress
  retry_cmd 5 30 aws_cli s3 cp "${final_manifest}" "s3://${SCFUZZBENCH_S3_BUCKET}/runs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}/manifest.json" --no-progress
}

upload_run_status_json() {
  local status=$1
  local meta
  meta=$(get_run_meta)

  local requested
  local succeeded
  local failed
  local max_parallel
  local completed_at
  requested=$(run_meta_num "${meta}" "requested_shards" 0)
  succeeded=$(run_meta_num "${meta}" "succeeded_count" 0)
  failed=$(run_meta_num "${meta}" "failed_count" 0)
  max_parallel=$(run_meta_num "${meta}" "max_parallel_effective" 0)
  completed_at=$(date -Is)

  local status_file="${SCFUZZBENCH_ROOT}/run-status.json"
  jq -cn \
    --arg status "${status}" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg completed_at "${completed_at}" \
    --arg requested "${requested}" \
    --arg succeeded "${succeeded}" \
    --arg failed "${failed}" \
    --arg max_parallel "${max_parallel}" \
    '{
      status:$status,
      run_id:$run_id,
      benchmark_uuid:$benchmark_uuid,
      requested_shards:($requested|tonumber),
      succeeded_count:($succeeded|tonumber),
      failed_count:($failed|tonumber),
      max_parallel_effective:($max_parallel|tonumber),
      completed_at:$completed_at
    }' >"${status_file}"

  retry_cmd 5 30 aws_cli s3 cp "${status_file}" "s3://${SCFUZZBENCH_S3_BUCKET}/logs/${STATUS_PREFIX}/status.json" --no-progress
  retry_cmd 5 30 aws_cli s3 cp "${status_file}" "s3://${SCFUZZBENCH_S3_BUCKET}/runs/${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}/status.json" --no-progress
  if ! upload_final_manifest_json "${status}" "${requested}" "${succeeded}" "${failed}" "${completed_at}"; then
    log "status.json uploaded, but final manifest merge/upload failed."
  fi
}

queue_counts() {
  local attrs
  attrs=$(aws_cli sqs get-queue-attributes \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output json)
  local visible
  local inflight
  visible=$(jq -r '.Attributes.ApproximateNumberOfMessages // "0"' <<<"${attrs}")
  inflight=$(jq -r '.Attributes.ApproximateNumberOfMessagesNotVisible // "0"' <<<"${attrs}")
  if ! [[ "${visible}" =~ ^[0-9]+$ ]]; then
    visible=0
  fi
  if ! [[ "${inflight}" =~ ^[0-9]+$ ]]; then
    inflight=0
  fi
  echo "${visible} ${inflight}"
}

backoff_seconds() {
  local attempt=$1
  local exponent=$((attempt - 1))
  if (( exponent < 0 )); then
    exponent=0
  fi
  local raw=$((SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS * (2 ** exponent)))
  if (( raw > SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS )); then
    raw=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}
  fi
  local jitter=$((RANDOM % (SCFUZZBENCH_SHARD_RETRY_BASE_SECONDS + 1)))
  local delay=$((raw + jitter))
  if (( delay > SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS )); then
    delay=${SCFUZZBENCH_SHARD_RETRY_MAX_SECONDS}
  fi
  echo "${delay}"
}

heartbeat_visibility() {
  local receipt_handle=$1
  while true; do
    sleep "${SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS}" || true
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" \
      >/dev/null || true
  done
}

heartbeat_shard_state() {
  local shard_key=$1
  local fuzzer_key=$2
  local run_index=$3
  local attempts=$4
  while true; do
    sleep "${SCFUZZBENCH_VISIBILITY_HEARTBEAT_SECONDS}" || true
    local now
    now=$(date -Is)
    local names
    local values
    names=$(jq -cn '{"#status":"status"}')
    values=$(jq -cn \
      --arg running "running" \
      --arg now "${now}" \
      --arg fuzzer_key "${fuzzer_key}" \
      --arg run_index "${run_index}" \
      --arg attempts "${attempts}" \
      --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
      '{
        ":running":{S:$running},
        ":now":{S:$now},
        ":fuzzer_key":{S:$fuzzer_key},
        ":run_index":{N:$run_index},
        ":attempts":{N:$attempts},
        ":instance_id":{S:$instance_id}
      }')
    aws_cli dynamodb update-item \
      --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
      --key "$(shard_key_json "${shard_key}")" \
      --condition-expression "#status = :running AND attempts = :attempts" \
      --update-expression "SET updated_at = :now, fuzzer_key = :fuzzer_key, run_index = :run_index, instance_id = :instance_id" \
      --expression-attribute-names "${names}" \
      --expression-attribute-values "${values}" \
      >/dev/null 2>&1 || true
  done
}

ensure_fuzzer_installed() {
  local fuzzer_key=$1
  local install_script="/opt/scfuzzbench/fuzzers/${fuzzer_key}/install.sh"
  local stamp_file="/opt/scfuzzbench/fuzzers/${fuzzer_key}/.installed"

  if [[ ! -x "${install_script}" ]]; then
    log "Missing install script for fuzzer '${fuzzer_key}'"
    return 1
  fi
  if [[ -f "${stamp_file}" ]]; then
    return 0
  fi

  log "Installing fuzzer '${fuzzer_key}'"
  bash "${install_script}"
  touch "${stamp_file}"
}

mark_run_completed_if_possible() {
  local visible inflight
  read -r visible inflight <<<"$(queue_counts)"
  if (( visible > 0 || inflight > 0 )); then
    return 1
  fi

  local meta
  meta=$(get_run_meta)
  local status
  local requested
  local succeeded
  local failed
  status=$(run_meta_status "${meta}")
  requested=$(run_meta_num "${meta}" "requested_shards" 0)
  succeeded=$(run_meta_num "${meta}" "succeeded_count" 0)
  failed=$(run_meta_num "${meta}" "failed_count" 0)

  if [[ "${status}" == "completed" || "${status}" == "failed" ]]; then
    if release_global_lock; then
      return 0
    fi
    return 1
  fi
  if (( succeeded + failed < requested )); then
    return 1
  fi

  if ! upload_run_status_json "completed"; then
    log "Failed to publish terminal status.json; keeping run in running state for retry."
    return 1
  fi

  local status_rc=0
  set_run_status "completed" || status_rc=$?
  if (( status_rc > 1 )); then
    log "Failed to persist terminal run-state status after publishing status.json."
    return 1
  fi

  log "Run completed: succeeded=${succeeded}, failed=${failed}, requested=${requested}"
  if ! release_global_lock; then
    log "Run status is terminal but lock release failed; will retry."
    return 1
  fi
  return 0
}

run_shard() {
  local receipt_handle=$1
  local body=$2
  local receive_count=$3

  local shard_key
  local fuzzer_key
  local run_index
  shard_key=$(jq -r '.shard_key // empty' <<<"${body}")
  fuzzer_key=$(jq -r '.fuzzer_key // empty' <<<"${body}")
  run_index=$(jq -r '.run_index // empty' <<<"${body}")

  if [[ -z "${shard_key}" || -z "${fuzzer_key}" || -z "${run_index}" ]]; then
    log "Invalid shard message body: ${body}"
    delete_message_safe "${receipt_handle}"
    return 0
  fi
  if ! [[ "${run_index}" =~ ^[0-9]+$ ]]; then
    log "Invalid run_index in shard message: ${body}"
    delete_message_safe "${receipt_handle}"
    return 0
  fi

  local claimed_attempt=""
  local claim_rc=0
  claimed_attempt=$(claim_shard_for_processing "${shard_key}" "${fuzzer_key}" "${run_index}") || claim_rc=$?
  if (( claim_rc != 0 )); then
    if (( claim_rc == 1 )); then
      handle_unclaimable_message "${shard_key}" "${receipt_handle}" "claim" "${fuzzer_key}" "${run_index}"
      return 0
    fi
    log "Failed to claim shard '${shard_key}' due to transient error; keeping message for retry."
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout 30 \
      >/dev/null || true
    return 0
  fi

  if ! [[ "${claimed_attempt}" =~ ^[0-9]+$ ]] || (( claimed_attempt < 1 )); then
    log "Invalid claimed attempt for shard '${shard_key}': ${claimed_attempt}; defaulting to 1."
    claimed_attempt=1
  fi

  if ! ensure_fuzzer_installed "${fuzzer_key}"; then
    local install_exit_code=200
    if (( claimed_attempt >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
      local install_terminal_rc=0
      retry_shard_transition 4 complete_shard_and_increment_counter "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" "failed" "${install_exit_code}" "install_failed_terminal" "failed_count" || install_terminal_rc=$?
      if (( install_terminal_rc == 0 )); then
        if [[ -n "${SCFUZZBENCH_QUEUE_DLQ_URL:-}" ]]; then
          aws_cli sqs send-message --queue-url "${SCFUZZBENCH_QUEUE_DLQ_URL}" --message-body "${body}" >/dev/null || true
        fi
        delete_message_safe "${receipt_handle}"
        mark_run_completed_if_possible || true
        return 0
      fi
      if (( install_terminal_rc == 1 )); then
        handle_unclaimable_message "${shard_key}" "${receipt_handle}" "install-failure terminal finalize" "${fuzzer_key}" "${run_index}"
        return 0
      fi
      aws_cli sqs change-message-visibility \
        --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
        --receipt-handle "${receipt_handle}" \
        --visibility-timeout 30 \
        >/dev/null || true
      return 0
    fi

    local install_delay
    install_delay=$(backoff_seconds "${claimed_attempt}")
    local install_retry_rc=0
    retry_shard_transition 4 mark_shard_retrying "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" "${install_exit_code}" "install_failed_retry_in_${install_delay}s" || install_retry_rc=$?
    if (( install_retry_rc == 0 )); then
      aws_cli sqs change-message-visibility \
        --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
        --receipt-handle "${receipt_handle}" \
        --visibility-timeout "${install_delay}" \
        >/dev/null || true
      return 0
    fi
    if (( install_retry_rc == 1 )); then
      handle_unclaimable_message "${shard_key}" "${receipt_handle}" "install-failure retry transition" "${fuzzer_key}" "${run_index}"
      return 0
    fi
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout 30 \
      >/dev/null || true
    return 0
  fi

  aws_cli sqs change-message-visibility \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --receipt-handle "${receipt_handle}" \
    --visibility-timeout "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" \
    >/dev/null || true

  heartbeat_visibility "${receipt_handle}" &
  local visibility_heartbeat_pid=$!
  heartbeat_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" &
  local shard_heartbeat_pid=$!

  local shard_root="/opt/scfuzzbench/shards/${shard_key}-a${claimed_attempt}-r${receive_count}"
  local artifact_suffix="${shard_key}-a${claimed_attempt}-r${receive_count}"
  rm -rf "${shard_root}" || true
  mkdir -p "${shard_root}"

  set +e
  SCFUZZBENCH_QUEUE_MODE=1 \
  SCFUZZBENCH_UPLOAD_DONE= \
  SCFUZZBENCH_ARTIFACT_SUFFIX="${artifact_suffix}" \
  SCFUZZBENCH_ROOT="${shard_root}" \
  SCFUZZBENCH_WORKDIR="${shard_root}/work" \
  SCFUZZBENCH_LOG_DIR="${shard_root}/logs" \
  bash "/opt/scfuzzbench/fuzzers/${fuzzer_key}/run.sh"
  local exit_code=$?
  set -e

  kill "${visibility_heartbeat_pid}" >/dev/null 2>&1 || true
  wait "${visibility_heartbeat_pid}" >/dev/null 2>&1 || true
  kill "${shard_heartbeat_pid}" >/dev/null 2>&1 || true
  wait "${shard_heartbeat_pid}" >/dev/null 2>&1 || true

  rm -rf "${shard_root}" || true

  if [[ "${exit_code}" -eq 0 ]]; then
    local success_finalize_rc=0
    retry_shard_transition 4 complete_shard_and_increment_counter "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" "succeeded" "0" "" "succeeded_count" || success_finalize_rc=$?
    if (( success_finalize_rc == 0 )); then
      delete_message_safe "${receipt_handle}"
      mark_run_completed_if_possible || true
      return 0
    fi
    if (( success_finalize_rc == 1 )); then
      handle_unclaimable_message "${shard_key}" "${receipt_handle}" "success finalize" "${fuzzer_key}" "${run_index}"
      return 0
    fi
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout 30 \
      >/dev/null || true
    return 0
  fi

  local failure_status="failed"
  if [[ "${exit_code}" -eq 124 || "${exit_code}" -eq 137 ]]; then
    failure_status="timed_out"
  fi

  if (( claimed_attempt >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
    local terminal_finalize_rc=0
    retry_shard_transition 4 complete_shard_and_increment_counter "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" "${failure_status}" "${exit_code}" "terminal" "failed_count" || terminal_finalize_rc=$?
    if (( terminal_finalize_rc == 0 )); then
      if [[ -n "${SCFUZZBENCH_QUEUE_DLQ_URL:-}" ]]; then
        aws_cli sqs send-message --queue-url "${SCFUZZBENCH_QUEUE_DLQ_URL}" --message-body "${body}" >/dev/null || true
      fi
      delete_message_safe "${receipt_handle}"
      mark_run_completed_if_possible || true
      return 0
    fi
    if (( terminal_finalize_rc == 1 )); then
      handle_unclaimable_message "${shard_key}" "${receipt_handle}" "terminal finalize" "${fuzzer_key}" "${run_index}"
      return 0
    fi
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout 30 \
      >/dev/null || true
    return 0
  fi

  local delay
  delay=$(backoff_seconds "${claimed_attempt}")
  local retry_rc=0
  retry_shard_transition 4 mark_shard_retrying "${shard_key}" "${fuzzer_key}" "${run_index}" "${claimed_attempt}" "${exit_code}" "retry_in_${delay}s" || retry_rc=$?
  if (( retry_rc == 0 )); then
    aws_cli sqs change-message-visibility \
      --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
      --receipt-handle "${receipt_handle}" \
      --visibility-timeout "${delay}" \
      >/dev/null || true
    return 0
  fi
  if (( retry_rc == 1 )); then
    handle_unclaimable_message "${shard_key}" "${receipt_handle}" "retry transition" "${fuzzer_key}" "${run_index}"
    return 0
  fi
  aws_cli sqs change-message-visibility \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --receipt-handle "${receipt_handle}" \
    --visibility-timeout 30 \
    >/dev/null || true
  return 0
}

cleanup_lock_heartbeat() {
  if [[ -n "${LOCK_HEARTBEAT_PID:-}" ]]; then
    kill "${LOCK_HEARTBEAT_PID}" >/dev/null 2>&1 || true
    wait "${LOCK_HEARTBEAT_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup_lock_heartbeat EXIT

log "Starting queue worker on instance ${SCFUZZBENCH_INSTANCE_ID:-unknown}"
WORKER_PARENT_PID=$$
rm -f "${LOCK_HEARTBEAT_FAILURE_MARKER}" || true
if ! extend_global_lock_lease >/dev/null 2>&1; then
  log "Failed to extend global lock lease before queue processing; failing closed."
  mark_run_failed_due_to_lock_error "initial_lock_lease_extend_failed" || true
  exit 1
fi
heartbeat_lock_lease &
LOCK_HEARTBEAT_PID=$!

idle_polls=0
while true; do
  if [[ -f "${LOCK_HEARTBEAT_FAILURE_MARKER}" ]]; then
    log "Detected lock heartbeat failure marker; stopping queue worker."
    exit 1
  fi
  message_json=$(aws_cli sqs receive-message \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --max-number-of-messages 1 \
    --wait-time-seconds "${SCFUZZBENCH_QUEUE_WAIT_SECONDS}" \
    --attribute-names All \
    --message-attribute-names All \
    --output json)

  receipt_handle=$(jq -r '.Messages[0].ReceiptHandle // empty' <<<"${message_json}")
  if [[ -z "${receipt_handle}" ]]; then
    read -r visible inflight <<<"$(queue_counts)"
    if (( visible == 0 && inflight == 0 )); then
      idle_polls=$((idle_polls + 1))
      if mark_run_completed_if_possible; then
        log "Queue drained and run marked complete."
        break
      fi
      if (( idle_polls >= SCFUZZBENCH_QUEUE_IDLE_POLLS )); then
        log "Queue appears idle but run is not yet complete; continuing to poll."
      fi
    else
      idle_polls=0
    fi
    sleep "${SCFUZZBENCH_QUEUE_EMPTY_SLEEP_SECONDS}" || true
    continue
  fi

  idle_polls=0
  body=$(jq -r '.Messages[0].Body // "{}"' <<<"${message_json}")
  receive_count=$(jq -r '.Messages[0].Attributes.ApproximateReceiveCount // "1"' <<<"${message_json}")
  if ! [[ "${receive_count}" =~ ^[0-9]+$ ]]; then
    receive_count=1
  fi

  run_shard "${receipt_handle}" "${body}" "${receive_count}"
done

log "Queue worker completed; shutting down instance"
shutdown_instance
