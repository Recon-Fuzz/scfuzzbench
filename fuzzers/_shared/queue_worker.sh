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

RUN_PK="RUN#${SCFUZZBENCH_RUN_ID}#${SCFUZZBENCH_BENCHMARK_UUID}"
STATUS_PREFIX="${SCFUZZBENCH_RUN_ID}/${SCFUZZBENCH_BENCHMARK_UUID}"

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

put_shard_state() {
  local shard_key=$1
  local fuzzer_key=$2
  local run_index=$3
  local status=$4
  local attempts=$5
  local exit_code=$6
  local error_message=$7
  local now
  now=$(date -Is)

  local item
  item=$(jq -cn \
    --arg pk "${RUN_PK}" \
    --arg sk "SHARD#${shard_key}" \
    --arg shard_key "${shard_key}" \
    --arg fuzzer_key "${fuzzer_key}" \
    --arg run_index "${run_index}" \
    --arg status "${status}" \
    --arg attempts "${attempts}" \
    --arg instance_id "${SCFUZZBENCH_INSTANCE_ID:-unknown}" \
    --arg updated_at "${now}" \
    --arg exit_code "${exit_code}" \
    --arg error_message "${error_message}" \
    '{
      pk:{S:$pk},
      sk:{S:$sk},
      entity_type:{S:"shard"},
      shard_key:{S:$shard_key},
      fuzzer_key:{S:$fuzzer_key},
      run_index:{N:$run_index},
      status:{S:$status},
      attempts:{N:$attempts},
      instance_id:{S:$instance_id},
      updated_at:{S:$updated_at}
    }
    + (if $exit_code != "" then {last_exit_code:{N:$exit_code}} else {} end)
    + (if $error_message != "" then {last_error:{S:$error_message}} else {} end)
    ')

  aws_cli dynamodb put-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --item "${item}" \
    >/dev/null
}

add_run_counter() {
  local field=$1
  local values
  values=$(jq -cn --arg now "$(date -Is)" '{":one":{N:"1"},":now":{S:$now}}')
  aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(meta_key_json)" \
    --update-expression "ADD ${field} :one SET updated_at = :now" \
    --expression-attribute-values "${values}" \
    >/dev/null
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

  aws_cli dynamodb update-item \
    --table-name "${SCFUZZBENCH_RUN_STATE_TABLE}" \
    --key "$(meta_key_json)" \
    --condition-expression "#status = :running" \
    --update-expression "SET #status = :status, completed_at = :now, updated_at = :now" \
    --expression-attribute-names "${names}" \
    --expression-attribute-values "${values}" \
    >/dev/null
}

release_global_lock() {
  local values
  values=$(jq -cn --arg owner "${SCFUZZBENCH_RUN_ID}" '{":owner":{S:$owner}}')
  local key
  key=$(jq -cn --arg name "${SCFUZZBENCH_LOCK_NAME}" '{lock_name:{S:$name}}')
  aws_cli dynamodb delete-item \
    --table-name "${SCFUZZBENCH_LOCK_TABLE}" \
    --key "${key}" \
    --condition-expression "owner_run_id = :owner" \
    --expression-attribute-values "${values}" \
    >/dev/null || true
}

upload_run_status_json() {
  local status=$1
  local meta
  meta=$(get_run_meta)

  local requested
  local succeeded
  local failed
  local max_parallel
  requested=$(run_meta_num "${meta}" "requested_shards" 0)
  succeeded=$(run_meta_num "${meta}" "succeeded_count" 0)
  failed=$(run_meta_num "${meta}" "failed_count" 0)
  max_parallel=$(run_meta_num "${meta}" "max_parallel_effective" 0)

  local status_file="${SCFUZZBENCH_ROOT}/run-status.json"
  jq -cn \
    --arg status "${status}" \
    --arg run_id "${SCFUZZBENCH_RUN_ID}" \
    --arg benchmark_uuid "${SCFUZZBENCH_BENCHMARK_UUID}" \
    --arg completed_at "$(date -Is)" \
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
    return 0
  fi
  if (( succeeded + failed < requested )); then
    return 1
  fi

  if set_run_status "completed"; then
    log "Run completed: succeeded=${succeeded}, failed=${failed}, requested=${requested}"
    upload_run_status_json "completed"
    release_global_lock
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
    aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
    return 0
  fi
  if ! [[ "${run_index}" =~ ^[0-9]+$ ]]; then
    log "Invalid run_index in shard message: ${body}"
    aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
    return 0
  fi

  put_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "running" "${receive_count}" "" ""

  if ! ensure_fuzzer_installed "${fuzzer_key}"; then
    put_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "failed" "${receive_count}" "200" "install_failed"
    add_run_counter "failed_count"
    aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
    mark_run_completed_if_possible || true
    return 0
  fi

  aws_cli sqs change-message-visibility \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --receipt-handle "${receipt_handle}" \
    --visibility-timeout "${SCFUZZBENCH_VISIBILITY_EXTENSION_SECONDS}" \
    >/dev/null || true

  heartbeat_visibility "${receipt_handle}" &
  local heartbeat_pid=$!

  local shard_root="/opt/scfuzzbench/shards/${shard_key}-a${receive_count}"
  local artifact_suffix="${shard_key}-a${receive_count}"
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

  kill "${heartbeat_pid}" >/dev/null 2>&1 || true
  wait "${heartbeat_pid}" >/dev/null 2>&1 || true

  rm -rf "${shard_root}" || true

  if [[ "${exit_code}" -eq 0 ]]; then
    put_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "succeeded" "${receive_count}" "0" ""
    add_run_counter "succeeded_count"
    aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
    mark_run_completed_if_possible || true
    return 0
  fi

  local failure_status="failed"
  if [[ "${exit_code}" -eq 124 || "${exit_code}" -eq 137 ]]; then
    failure_status="timed_out"
  fi

  if (( receive_count >= SCFUZZBENCH_SHARD_MAX_ATTEMPTS )); then
    put_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "${failure_status}" "${receive_count}" "${exit_code}" "terminal"
    add_run_counter "failed_count"
    if [[ -n "${SCFUZZBENCH_QUEUE_DLQ_URL:-}" ]]; then
      aws_cli sqs send-message --queue-url "${SCFUZZBENCH_QUEUE_DLQ_URL}" --message-body "${body}" >/dev/null || true
    fi
    aws_cli sqs delete-message --queue-url "${SCFUZZBENCH_QUEUE_URL}" --receipt-handle "${receipt_handle}" >/dev/null || true
    mark_run_completed_if_possible || true
    return 0
  fi

  local delay
  delay=$(backoff_seconds "${receive_count}")
  put_shard_state "${shard_key}" "${fuzzer_key}" "${run_index}" "retrying" "${receive_count}" "${exit_code}" "retry_in_${delay}s"
  aws_cli sqs change-message-visibility \
    --queue-url "${SCFUZZBENCH_QUEUE_URL}" \
    --receipt-handle "${receipt_handle}" \
    --visibility-timeout "${delay}" \
    >/dev/null || true
  return 0
}

log "Starting queue worker on instance ${SCFUZZBENCH_INSTANCE_ID:-unknown}"

idle_polls=0
while true; do
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
