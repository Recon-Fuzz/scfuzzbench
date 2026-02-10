---
name: Benchmark request
about: Request a new scfuzzbench benchmark run (requires maintainer approval).
title: "benchmark: <org>/<repo>@<ref>"
labels: "benchmark/needs-approval"
---

<!-- scfuzzbench-benchmark-request:v1 -->

Paste a JSON request below.

Notes:
- Do not include secrets in this issue.
- A maintainer must change the label from `benchmark/needs-approval` to `benchmark/approved` to start the run.
- Limits: `instances_per_fuzzer` must be in `[1, 20]`, `timeout_hours` must be in `[0.25, 72]`.

```json
{
  "target_repo_url": "https://github.com/Recon-Fuzz/aave-v4-scfuzzbench",
  "target_commit": "v0.5.6-recon",
  "benchmark_type": "property",
  "instance_type": "c6a.4xlarge",
  "instances_per_fuzzer": 4,
  "timeout_hours": 1,
  "foundry_version": "",
  "foundry_git_repo": "https://github.com/aviggiano/foundry",
  "foundry_git_ref": "master",
  "echidna_version": "",
  "medusa_version": "",
  "bitwuzla_version": "",
  "git_token_ssm_parameter_name": "/scfuzzbench/recon/github_token",
  "properties_path": "",
  "fuzzer_env_json": ""
}
```
