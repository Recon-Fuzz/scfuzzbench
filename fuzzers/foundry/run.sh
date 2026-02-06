#!/usr/bin/env bash
set -euo pipefail

source /opt/scfuzzbench/common.sh

register_shutdown_trap

prepare_workspace
export PATH="/root/.foundry/bin:${PATH}"

if [[ -n "${FOUNDRY_LABEL:-}" ]]; then
  SCFUZZBENCH_FUZZER_LABEL="${FOUNDRY_LABEL}"
elif [[ -f /opt/scfuzzbench/foundry_commit ]]; then
  foundry_commit=$(cat /opt/scfuzzbench/foundry_commit)
  SCFUZZBENCH_FUZZER_LABEL="foundry-git-${foundry_commit}"
else
  require_env FOUNDRY_VERSION
  SCFUZZBENCH_FUZZER_LABEL="foundry-${FOUNDRY_VERSION}"
fi
export SCFUZZBENCH_FUZZER_LABEL

clone_target
apply_benchmark_type
build_target

repo_dir="${SCFUZZBENCH_WORKDIR}/target"
log_file="${SCFUZZBENCH_LOG_DIR}/foundry.log"

extra_args=()
if [[ -n "${FOUNDRY_TEST_ARGS:-}" ]]; then
  read -r -a extra_args <<< "${FOUNDRY_TEST_ARGS}"
fi

set +e
pushd "${repo_dir}" >/dev/null
run_with_timeout "${log_file}" forge test --mc CryticToFoundry "${extra_args[@]}"
exit_code=$?
popd >/dev/null
set -e

upload_results
exit ${exit_code}
