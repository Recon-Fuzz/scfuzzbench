#!/usr/bin/env bash
set -euo pipefail

SCFUZZBENCH_ROOT=${SCFUZZBENCH_ROOT:-/opt/scfuzzbench}
SCFUZZBENCH_WORKDIR=${SCFUZZBENCH_WORKDIR:-${SCFUZZBENCH_ROOT}/work}
SCFUZZBENCH_LOG_DIR=${SCFUZZBENCH_LOG_DIR:-${SCFUZZBENCH_ROOT}/logs}
SCFUZZBENCH_CORPUS_DIR=${SCFUZZBENCH_CORPUS_DIR:-}
SCFUZZBENCH_BENCHMARK_TYPE=${SCFUZZBENCH_BENCHMARK_TYPE:-property}
SCFUZZBENCH_BENCHMARK_UUID=${SCFUZZBENCH_BENCHMARK_UUID:-}
SCFUZZBENCH_BENCHMARK_MANIFEST_B64=${SCFUZZBENCH_BENCHMARK_MANIFEST_B64:-}
SCFUZZBENCH_PROPERTIES_PATH=${SCFUZZBENCH_PROPERTIES_PATH:-}

log() {
  echo "[$(date -Is)] $*"
}

require_env() {
  local name
  for name in "$@"; do
    if [[ -z "${!name:-}" ]]; then
      log "Missing required env var: ${name}"
      exit 1
    fi
  done
}

prepare_workspace() {
  mkdir -p "${SCFUZZBENCH_ROOT}" "${SCFUZZBENCH_WORKDIR}" "${SCFUZZBENCH_LOG_DIR}"
}

install_shutdown_script() {
  local shutdown_path="${SCFUZZBENCH_ROOT}/shutdown.sh"
  if [[ -f "${shutdown_path}" ]]; then
    return 0
  fi
  cat <<'SHUTDOWN' >"${shutdown_path}"
#!/usr/bin/env bash
set +e

log() {
  echo "[$(date -Is)] $*"
}

log "Shutting down instance"
sync || true
shutdown -h now || systemctl poweroff || halt -p || true
SHUTDOWN
  chmod +x "${shutdown_path}"
}

shutdown_instance() {
  install_shutdown_script
  local delay="${SCFUZZBENCH_SHUTDOWN_GRACE_SECONDS:-0}"
  if [[ "${delay}" =~ ^[0-9]+$ ]] && [[ "${delay}" -gt 0 ]]; then
    log "Delaying shutdown for ${delay}s"
    sleep "${delay}" || true
  fi
  "${SCFUZZBENCH_ROOT}/shutdown.sh" || true
}

finalize_run() {
  local exit_code=$?
  set +e
  if [[ -z "${SCFUZZBENCH_UPLOAD_DONE:-}" ]]; then
    if [[ -n "${SCFUZZBENCH_S3_BUCKET:-}" && -n "${SCFUZZBENCH_RUN_ID:-}" && -n "${SCFUZZBENCH_FUZZER_LABEL:-}" ]]; then
      upload_results || true
    else
      log "Skipping upload in finalize; missing S3 bucket, run id, or fuzzer label."
    fi
  fi
  shutdown_instance
  return ${exit_code}
}

register_shutdown_trap() {
  install_shutdown_script
  trap finalize_run EXIT
}

install_base_packages() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y \
    ca-certificates \
    curl \
    git \
    jq \
    tar \
    zip \
    unzip \
    build-essential \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    python3-venv

  if ! command -v aws >/dev/null 2>&1; then
    log "Installing AWS CLI v2"
    local tmp_dir
    tmp_dir=$(mktemp -d)
    curl -sSfL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "${tmp_dir}/awscliv2.zip"
    unzip -q "${tmp_dir}/awscliv2.zip" -d "${tmp_dir}"
    "${tmp_dir}/aws/install" --update
    rm -rf "${tmp_dir}"
    aws --version
  fi
}

install_foundry() {
  if [[ -n "${FOUNDRY_GIT_REPO:-}" ]]; then
    log "Installing Foundry from ${FOUNDRY_GIT_REPO}"
    export HOME=/root
    if ! command -v cargo >/dev/null 2>&1; then
      log "Installing Rust toolchain"
      curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
      # shellcheck source=/dev/null
      source /root/.cargo/env
    fi
    local tmp_dir
    tmp_dir=$(mktemp -d)
    git clone --depth 1 "${FOUNDRY_GIT_REPO}" "${tmp_dir}/foundry"
    if [[ -n "${FOUNDRY_GIT_REF:-}" ]]; then
      git -C "${tmp_dir}/foundry" fetch --depth 1 origin "${FOUNDRY_GIT_REF}"
      git -C "${tmp_dir}/foundry" checkout "${FOUNDRY_GIT_REF}"
    fi
    local commit
    commit=$(git -C "${tmp_dir}/foundry" rev-parse --short HEAD)
    log "Building Foundry at ${commit}"
    # shellcheck source=/dev/null
    source /root/.cargo/env
    cargo build --release --manifest-path "${tmp_dir}/foundry/Cargo.toml"
    install -m 0755 "${tmp_dir}/foundry/target/release/forge" /usr/local/bin/forge
    install -m 0755 "${tmp_dir}/foundry/target/release/cast" /usr/local/bin/cast
    install -m 0755 "${tmp_dir}/foundry/target/release/anvil" /usr/local/bin/anvil
    install -m 0755 "${tmp_dir}/foundry/target/release/chisel" /usr/local/bin/chisel || true
    echo "${commit}" > /opt/scfuzzbench/foundry_commit
    echo "${FOUNDRY_GIT_REPO}" > /opt/scfuzzbench/foundry_repo
    rm -rf "${tmp_dir}"
    forge --version
  else
    require_env FOUNDRY_VERSION
    log "Installing Foundry ${FOUNDRY_VERSION}"
    export HOME=/root
    curl -L https://foundry.paradigm.xyz | bash
    export PATH="/root/.foundry/bin:${PATH}"
    /root/.foundry/bin/foundryup -i "${FOUNDRY_VERSION}"
    forge --version
  fi
}

install_crytic_compile() {
  log "Installing crytic-compile"
  python3 -m pip install --no-cache-dir --break-system-packages crytic-compile
  command -v crytic-compile
}

install_slither_analyzer() {
  log "Installing slither-analyzer"
  python3 -m pip install --no-cache-dir --break-system-packages --ignore-installed slither-analyzer
  command -v slither
}

get_instance_id() {
  local token
  token=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
  curl -s -H "X-aws-ec2-metadata-token: ${token}" \
    http://169.254.169.254/latest/meta-data/instance-id
}

get_github_token() {
  if [[ -n "${SCFUZZBENCH_GIT_TOKEN:-}" ]]; then
    echo "${SCFUZZBENCH_GIT_TOKEN}"
    return 0
  fi
  if [[ -n "${SCFUZZBENCH_GIT_TOKEN_SSM_PARAMETER:-}" ]]; then
    aws ssm get-parameter --with-decryption --name "${SCFUZZBENCH_GIT_TOKEN_SSM_PARAMETER}" \
      --query 'Parameter.Value' --output text
    return 0
  fi
  return 1
}

clone_target() {
  require_env SCFUZZBENCH_REPO_URL SCFUZZBENCH_COMMIT
  local repo_dir="${SCFUZZBENCH_WORKDIR}/target"
  local git_token=""
  git_token=$(get_github_token 2>/dev/null || true)
  if [[ ! -d "${repo_dir}/.git" ]]; then
    if [[ -n "${git_token}" ]]; then
      local clone_url
      if [[ "${SCFUZZBENCH_REPO_URL}" == https://* ]]; then
        clone_url="https://x-access-token:${git_token}@${SCFUZZBENCH_REPO_URL#https://}"
      else
        clone_url="${SCFUZZBENCH_REPO_URL}"
      fi
      log "Cloning ${SCFUZZBENCH_REPO_URL} with GitHub token"
      GIT_TERMINAL_PROMPT=0 git clone "${clone_url}" "${repo_dir}"
      git -C "${repo_dir}" remote set-url origin "${clone_url}"
    else
      log "Cloning ${SCFUZZBENCH_REPO_URL}"
      git clone "${SCFUZZBENCH_REPO_URL}" "${repo_dir}"
    fi
  fi
  pushd "${repo_dir}" >/dev/null
  if [[ -n "${git_token}" ]]; then
    GIT_TERMINAL_PROMPT=0 git fetch --depth 1 origin "${SCFUZZBENCH_COMMIT}"
  else
    git fetch --depth 1 origin "${SCFUZZBENCH_COMMIT}"
  fi
  git checkout "${SCFUZZBENCH_COMMIT}"
  if [[ -f .gitmodules ]]; then
    log "Initializing git submodules"
    if [[ -n "${git_token}" ]]; then
      git config --local --add url."https://x-access-token:${git_token}@github.com/".insteadOf "https://github.com/"
      git config --local --add url."https://x-access-token:${git_token}@github.com/".insteadOf "git@github.com:"
      git config --local --add url."https://x-access-token:${git_token}@github.com/".insteadOf "ssh://git@github.com/"
      git config --local --add url."https://x-access-token:${git_token}@github.com/".insteadOf "git://github.com/"
      sed -i \
        -e 's#git@github.com:#https://github.com/#g' \
        -e 's#ssh://git@github.com/#https://github.com/#g' \
        -e 's#git://github.com/#https://github.com/#g' \
        .gitmodules
      git submodule sync --recursive
      GIT_TERMINAL_PROMPT=0 git -c url."https://x-access-token:${git_token}@github.com/".insteadOf="https://github.com/" \
        submodule update --init --recursive
    else
      git submodule update --init --recursive
    fi
  fi
  popd >/dev/null
}

apply_benchmark_type() {
  local repo_dir="${SCFUZZBENCH_WORKDIR}/target"
  local mode="${SCFUZZBENCH_BENCHMARK_TYPE}"
  local properties_path="${SCFUZZBENCH_PROPERTIES_PATH}"

  if [[ -z "${properties_path}" ]]; then
    log "SCFUZZBENCH_PROPERTIES_PATH not set; skipping benchmark mode switch."
    if [[ "${mode}" == "optimization" ]]; then
      log "Optimization mode requested, but SCFUZZBENCH_PROPERTIES_PATH is empty."
      return 1
    fi
    return 0
  fi

  local properties_file="${repo_dir}/${properties_path}"

  if [[ ! -f "${properties_file}" ]]; then
    log "Properties.sol not found at ${properties_file}; skipping benchmark mode switch."
    if [[ "${mode}" == "optimization" ]]; then
      log "Optimization mode requested, but Properties.sol is missing."
      return 1
    fi
    return 0
  fi

  if ! grep -q "OPTIMIZATION_MODE" "${properties_file}"; then
    log "OPTIMIZATION_MODE flag not found in Properties.sol; skipping benchmark mode switch."
    if [[ "${mode}" == "optimization" ]]; then
      log "Optimization mode requested, but Properties.sol does not support it."
      return 1
    fi
    return 0
  fi

  case "${mode}" in
    property)
      if grep -q "OPTIMIZATION_MODE = true" "${properties_file}" || grep -q "public returns (int256 maxViolation)" "${properties_file}"; then
        log "Switching benchmark to property mode"
        sed -i \
          -e 's/OPTIMIZATION_MODE = true/OPTIMIZATION_MODE = false/' \
          -e 's/public returns (int256 maxViolation)/public returns (bool)/g' \
          -e 's/return maxViolation;/return maxViolation <= 0;/g' \
          -e 's/optimize_/invariant_/g' \
          "${properties_file}"
      else
        log "Benchmark already in property mode"
      fi
      ;;
    optimization)
      if grep -q "OPTIMIZATION_MODE = false" "${properties_file}" || grep -q "public returns (bool)" "${properties_file}"; then
        log "Switching benchmark to optimization mode"
        sed -i \
          -e 's/OPTIMIZATION_MODE = false/OPTIMIZATION_MODE = true/' \
          -e 's/public returns (bool)/public returns (int256 maxViolation)/g' \
          -e 's/return maxViolation <= 0;/return maxViolation;/g' \
          -e 's/invariant_/optimize_/g' \
          "${properties_file}"
      else
        log "Benchmark already in optimization mode"
      fi
      ;;
    *)
      log "Unknown SCFUZZBENCH_BENCHMARK_TYPE: ${mode} (expected property or optimization)"
      return 1
      ;;
  esac
}

build_target() {
  local repo_dir="${SCFUZZBENCH_WORKDIR}/target"
  log "Building target with forge"
  pushd "${repo_dir}" >/dev/null
  if [[ ! -d "lib/forge-std" ]]; then
    log "Installing Foundry dependencies (forge install --no-commit)"
    forge install --no-commit || true
  fi
  forge build
  popd >/dev/null
}

run_with_timeout() {
  require_env SCFUZZBENCH_TIMEOUT_SECONDS
  local log_file=$1
  shift
  local kill_after="${SCFUZZBENCH_TIMEOUT_GRACE_SECONDS:-300}"
  if [[ ! "${kill_after}" =~ ^[0-9]+$ ]]; then
    kill_after=300
  fi
  log "Running command with timeout ${SCFUZZBENCH_TIMEOUT_SECONDS}s (grace ${kill_after}s)"
  set +e
  timeout --signal=SIGINT --kill-after="${kill_after}s" "${SCFUZZBENCH_TIMEOUT_SECONDS}s" "$@" 2>&1 | tee "${log_file}"
  local exit_code=${PIPESTATUS[0]}
  set -e
  return ${exit_code}
}

upload_results() {
  require_env SCFUZZBENCH_S3_BUCKET SCFUZZBENCH_RUN_ID SCFUZZBENCH_FUZZER_LABEL
  local instance_id
  instance_id=$(get_instance_id)
  local base_name="${instance_id}-${SCFUZZBENCH_FUZZER_LABEL}"
  local upload_dir="${SCFUZZBENCH_ROOT}/upload"
  mkdir -p "${upload_dir}"
  local log_zip="${upload_dir}/logs-${base_name}.zip"
  local prefix="${SCFUZZBENCH_RUN_ID}"
  if [[ -n "${SCFUZZBENCH_BENCHMARK_UUID}" ]]; then
    prefix="${SCFUZZBENCH_BENCHMARK_UUID}/${SCFUZZBENCH_RUN_ID}"
  fi
  local log_dest="s3://${SCFUZZBENCH_S3_BUCKET}/logs/${prefix}/${base_name}.zip"
  if [[ -d "${SCFUZZBENCH_LOG_DIR}" ]]; then
    log "Zipping logs to ${log_zip}"
    local log_parent
    local log_base
    log_parent=$(dirname "${SCFUZZBENCH_LOG_DIR}")
    log_base=$(basename "${SCFUZZBENCH_LOG_DIR}")
    (cd "${log_parent}" && zip -r -q "${log_zip}" "${log_base}")
    log "Uploading logs to ${log_dest}"
    aws s3 cp "${log_zip}" "${log_dest}" --no-progress
    if [[ -n "${SCFUZZBENCH_BENCHMARK_MANIFEST_B64}" ]]; then
      local manifest_path="${upload_dir}/benchmark_manifest.json"
      echo "${SCFUZZBENCH_BENCHMARK_MANIFEST_B64}" | base64 -d > "${manifest_path}"
      aws s3 cp "${manifest_path}" "s3://${SCFUZZBENCH_S3_BUCKET}/logs/${prefix}/manifest.json" --no-progress
    fi
  else
    log "No logs directory found; skipping log upload."
  fi

  if [[ -n "${SCFUZZBENCH_CORPUS_DIR}" && -d "${SCFUZZBENCH_CORPUS_DIR}" ]]; then
    local corpus_zip="${upload_dir}/corpus-${base_name}.zip"
    local corpus_dest="s3://${SCFUZZBENCH_S3_BUCKET}/corpus/${prefix}/${base_name}.zip"
    log "Zipping corpus to ${corpus_zip}"
    local corpus_parent
    local corpus_base
    corpus_parent=$(dirname "${SCFUZZBENCH_CORPUS_DIR}")
    corpus_base=$(basename "${SCFUZZBENCH_CORPUS_DIR}")
    (cd "${corpus_parent}" && zip -r -q "${corpus_zip}" "${corpus_base}")
    log "Uploading corpus to ${corpus_dest}"
    aws s3 cp "${corpus_zip}" "${corpus_dest}" --no-progress
  else
    log "No corpus directory configured or found; skipping corpus upload."
  fi

  export SCFUZZBENCH_UPLOAD_DONE=1
}
