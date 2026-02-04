# scfuzzbench

Benchmark suite for smart-contract fuzzers.

## How it works

Terraform in `infrastructure/` launches N identical EC2 instances per fuzzer. Each instance installs only its fuzzer (`fuzzers/<name>/install.sh`), clones the target repo at the pinned commit, runs `forge build`, then executes `run.sh` under the `timeout` wrapper so every run stops cleanly at the deadline. Logs are uploaded as `s3://<bucket>/logs/<benchmark_uuid>/<run_id>/i-XXXX-<fuzzer-version>.zip` and corpus output (when configured) as `s3://<bucket>/corpus/<benchmark_uuid>/<run_id>/i-XXXX-<fuzzer-version>.zip`, where `<run_id>` defaults to the unix timestamp at apply time. The `benchmark_uuid` is an MD5 of a manifest including the scfuzzbench commit, target repo/ref, benchmark type, instance type, and fuzzer versions; a `manifest.json` is uploaded alongside the logs.

Benchmark type: `benchmark_type` controls whether the run uses `property` (default) or `optimization` mode. The runner uses `SCFUZZBENCH_PROPERTIES_PATH` (set via `fuzzer_env`) to locate the properties file and applies the sed transformations noted in that file before building/running.

Analysis: `analysis/analyze.py` parses logs into CSV and generates charts (broken assertions over time per run, plus median/average per fuzzer). Install plotting deps with `pip install -r analysis/requirements.txt`.

CSV report: `analysis/benchmark_report.py` consumes a long-form CSV (`fuzzer, run_id, time_hours, bugs_found`) and produces a Markdown report plus plots (median+IQR curves, time-to-k, end distribution, plateau/late-share). Use `analysis/wide_to_long.py` if your CSV is in wide format.
`analysis/events_to_cumulative.py` converts `events.csv` (from log parsing) into this cumulative CSV format.

## Benchmark inputs

Set these inputs via `-var`/`tfvars` when you run Terraform (defaults are intentionally blank):

- `target_repo_url` and `target_commit`
- `benchmark_type` (`property` or `optimization`)
- `instance_type`, `instances_per_fuzzer`, `timeout_hours`
- fuzzer versions (and `foundry_git_repo`/`foundry_git_ref` if building from source)
- `git_token_ssm_parameter_name` if the target repo is private
- `SCFUZZBENCH_PROPERTIES_PATH` in `fuzzer_env` to point at the properties file used for mode switching

Per-fuzzer environment variables live in `fuzzers/README.md`.

Quick start:

```bash
make terraform-init
make terraform-deploy TF_ARGS="-var 'ssh_cidr=YOUR_IP/32' -var 'target_repo_url=REPO_URL' -var 'target_commit=COMMIT'"
```

Buckets are intended to be long-lived. If you want to reuse an existing bucket across runs, set `EXISTING_BUCKET=<bucket-name>` so Terraform uses `existing_bucket_name` and only the `<benchmark_uuid>/<run_id>` prefix changes. Avoid purging the bucket unless you explicitly want to delete historical runs.

If you need to tear down infra but keep the bucket, use:

```bash
make terraform-destroy-infra
```

After downloading logs and unzip:

```bash
make results-analyze LOGS_DIR=/tmp/logs-xxx OUT_DIR=analysis_out RUN_ID=1770053924
```

If you reuse an existing bucket, set `EXISTING_BUCKET=<bucket-name>` so Terraform uses `existing_bucket_name`. To download artifacts from the new layout, pass the benchmark UUID:

```bash
make results-download BUCKET=<bucket-name> RUN_ID=1770053924 BENCHMARK_UUID=<benchmark_uuid> ARTIFACT_CATEGORY=both
```

You can read `benchmark_uuid` (and `run_id`) from `terraform output`.

CSV report (example):

```bash
make report-benchmark REPORT_CSV=results.csv REPORT_OUT_DIR=report_out REPORT_BUDGET=24
```

If you need to clone a private target repo, store a short-lived token in SSM and set `git_token_ssm_parameter_name` so the instances can fetch it without embedding secrets in user-data logs.
