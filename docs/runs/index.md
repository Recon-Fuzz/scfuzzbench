# Runs

This page is generated in CI from the S3 run index (`runs/<run_id>/<benchmark_uuid>/manifest.json`).

::: tip
Only **complete** runs are shown (terminal run status for queue mode, with legacy timeout + 1h grace fallback).

If you are previewing locally, run the generator first:

```bash
python3 scripts/generate_docs_site.py --bucket "$SCFUZZBENCH_BUCKET" --region "$AWS_REGION"
```
:::
