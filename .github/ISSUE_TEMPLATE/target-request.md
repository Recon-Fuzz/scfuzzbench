---
name: Target request
about: Request onboarding of a new benchmark target into Recon-Fuzz/scfuzzbench.
title: "target: <org>/<repo>@<ref> -> Recon-Fuzz/<repo>-scfuzzbench"
---

<!-- scfuzzbench-target-request:v1 -->

Paste a target request JSON payload below.

References:
- https://scfuzzbench.com/targets
- https://github.com/Recon-Fuzz/scfuzzbench/blob/main/docs/targets.md

```json
{
  "upstream_target_repo_url": "https://github.com/superform-xyz/v2-periphery",
  "vulnerable_baseline_commit_sha_for_dev": "79c946332f72266ba0eeb7a3e062de371139e477",
  "recon_harness_source_repo_url": "https://github.com/superform-xyz/v2-periphery",
  "recon_harness_source_ref_for_test_recon": "dev",
  "destination_repo_url": "https://github.com/Recon-Fuzz/superform-v2-periphery-scfuzzbench",
  "base_branch_name": "dev",
  "recon_branch_name": "dev-recon",
  "benchmark_type": "property"
}
```

