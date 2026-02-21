# Start Benchmark

This page creates a **benchmark request** issue in GitHub.

Need a new target first? Use the target onboarding skill at
[`skills/target-onboarding/SKILL.md`](https://github.com/Recon-Fuzz/scfuzzbench/blob/main/skills/target-onboarding/SKILL.md)
and follow its workflow.

The request moves through GitHub labels:

- `benchmark/01-pending`: added by the issue template on creation.
- `benchmark/02-validated`: added by the bot after JSON validation passes.
- `benchmark/03-approved`: added manually by a maintainer.

<StartBenchmark />

::: warning
Do not put secrets in the issue body. The request is intentionally public/auditable.
:::
