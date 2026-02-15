---
layout: home
hero:
  name: scfuzzbench
  text: Smart-contract fuzzer benchmarks
  tagline: Fully static, generated in CI from S3 run artifacts.
  actions:
    - theme: brand
      text: Browse runs
      link: /runs/
    - theme: alt
      text: Start benchmark
      link: /start
    - theme: alt
      text: Methodology
      link: /methodology
features:
  - title: Timestamp-first runs
    details: Run IDs are Unix timestamps, so “latest” is always obvious just by listing prefixes.
  - title: Complete runs only
    details: The site lists only complete runs (queue status for queue-mode runs, timeout+grace for legacy runs).
  - title: Triage-friendly
    details: Complete runs missing analysis stay visible with warnings and raw-artifact links.
---

::: tip Local preview
The `Runs` and `Benchmarks` pages are generated from S3 in CI.

To preview locally, generate pages with AWS credentials:

```bash
python3 scripts/generate_docs_site.py --bucket "$SCFUZZBENCH_BUCKET" --region "$AWS_REGION"
npm run docs:dev
```
:::

## What It Looks Like

<div class="sb-gallery">
  <img src="/images/sample-run/bugs_over_time.png" alt="Bugs over time chart sample" />
  <img src="/images/sample-run/time_to_k.png" alt="Time to K chart sample" />
  <img src="/images/sample-run/final_distribution.png" alt="Final distribution chart sample" />
  <img src="/images/sample-run/plateau_and_late_share.png" alt="Plateau and late share chart sample" />
</div>
