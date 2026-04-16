#!/usr/bin/env bash
set -euo pipefail

source /opt/scfuzzbench/common.sh

prepare_workspace
install_base_packages
install_foundry
install_crytic_compile
install_slither_analyzer

require_env RECON_VERSION
recon_version="${RECON_VERSION#v}"
log "Installing Recon fuzzer v${recon_version}"

tmp_dir=$(mktemp -d)
archive="recon-linux-x86_64.tar.gz"
url="https://github.com/Recon-Fuzz/recon-fuzzer/releases/download/v${recon_version}/${archive}"

curl -L "${url}" -o "${tmp_dir}/${archive}"
tar -xzf "${tmp_dir}/${archive}" -C "${tmp_dir}"

bin_path=$(find "${tmp_dir}" -type f -name "recon" | head -n 1)
if [[ -z "${bin_path}" ]]; then
  log "recon binary not found in archive"
  exit 1
fi
install -m 0755 "${bin_path}" /usr/local/bin/recon

rm -rf "${tmp_dir}"

command -v recon
recon --version || true
