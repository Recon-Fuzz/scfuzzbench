# Runs

This page is generated in CI from the S3 run index (`runs/<run_id>/<benchmark_uuid>/manifest.json`).

::: tip
Only **complete** runs are shown (queue status for queue-mode runs, timeout + 1h grace for legacy runs).

If you are previewing locally, run the generator first:

```bash
python3 scripts/generate_docs_site.py --bucket "$SCFUZZBENCH_BUCKET" --region "$AWS_REGION"
```
:::
