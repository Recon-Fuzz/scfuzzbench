# scfuzzbench

Benchmark suite for smart-contract fuzzers.

<table>
  <tr>
    <td><img src="docs/public/images/sample-run/bugs_over_time.png" alt="Bugs over time" width="420"></td>
    <td><img src="docs/public/images/sample-run/time_to_k.png" alt="Time to k" width="420"></td>
  </tr>
  <tr>
    <td><img src="docs/public/images/sample-run/final_distribution.png" alt="Final distribution" width="420"></td>
    <td><img src="docs/public/images/sample-run/plateau_and_late_share.png" alt="Plateau and late share" width="420"></td>
  </tr>
</table>

`scfuzzbench` provides an up-to-date, practical benchmark for smart-contract fuzzers focused on stateful invariant testing in realistic DeFi scenarios.

## Motivation

- Maintain a current view of common fuzzers under a shared, realistic workload.
- Focus on benchmark quality with real projects, real bug-finding tasks, long timeouts, and repeated runs.
- Publish transparent metrics and artifacts for independent review.
- Help fuzzer/tool builders identify bottlenecks and improve their tools.

## Inclusion Criteria For Fuzzers

A fuzzer is currently considered in-scope when it is:

- Open source.
- Able to run assertion failures.
- Able to run global invariants.

## Fuzzers Currently Ready

- Foundry
- Echidna
- Medusa

## Documentation

For all technical/operational details, use the docs site pages:

- Introduction: `docs/introduction.md`
- Start benchmark request: `docs/start.md`
- Methodology: `docs/methodology.md`
- Operations guide (Terraform, running, reruns, analysis, CI workflows): `docs/operations.md`

Rendered docs navigation and run/benchmark pages are available under `docs/`.
