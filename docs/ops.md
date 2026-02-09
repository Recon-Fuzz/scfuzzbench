# Ops Notes

## What Counts As a "Complete" Run

The docs site lists **only complete runs**.

A run is considered complete when:

- `now >= run_id + (timeout_hours * 3600) + 3600`

Where:

- `run_id` is a Unix timestamp (seconds).
- `timeout_hours` comes from the run's `manifest.json` (defaults to `24` if missing).
- `3600` is a fixed 1h grace period.

## Complete Run, Missing Analysis

If a run is **complete** but shows **Missing analysis**, it means the benchmark run produced logs in S3 but the analysis publishing step did not complete (or never ran).

Typical fixes:

1. Re-run the GitHub Actions **Benchmark Release** workflow for that `benchmark_uuid` + `run_id`.
2. Run analysis locally and publish the artifacts to S3 (advanced/manual):
   - `make results-analyze-all BUCKET=<bucket> RUN_ID=<run_id> BENCHMARK_UUID=<benchmark_uuid> DEST=<tmpdir> ARTIFACT_CATEGORY=both`
   - Upload `REPORT.md`, charts, and bundles to `s3://<bucket>/analysis/<benchmark_uuid>/<run_id>/...`
3. If the run is junk, delete its S3 prefixes (destructive):
   - `runs/<run_id>/<benchmark_uuid>/...`
   - `logs/<run_id>/<benchmark_uuid>/...`
   - `corpus/<run_id>/<benchmark_uuid>/...` (if present)
   - `analysis/<benchmark_uuid>/<run_id>/...` (if partially uploaded)

The docs site keeps these runs visible (with warnings) so maintainers can triage them later.
