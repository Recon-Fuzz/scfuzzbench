# Run `1771615839`

- Date (UTC): `2026-02-20 19:30:39Z`
- Benchmark: [`027e45da412aec12841579d664c97ff8`](../../../benchmarks/027e45da412aec12841579d664c97ff8/)
- Timeout: `12h`

## Charts

![Bugs Over Time](https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/bugs_over_time.png)
![Bugs Over Time (All Runs)](https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/bugs_over_time_runs.png)
![Time To K](https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/time_to_k.png)
![Final Distribution](https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/final_distribution.png)
![Plateau And Late Share](https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/plateau_and_late_share.png)
![Invariant Overlap (UpSet)](/images/generated/invariant_overlap_upset-1771615839-027e45da412aec12841579d664c97ff8.png)

## Report

### Fuzzer Benchmark Report (from bug-count CSV)

- Time budget: **12.00h**

#### Executive summary
This report is derived solely from cumulative bugs-found over time across repeated runs per fuzzer. It emphasizes robust, distribution-based metrics (median/IQR, success rates, time-to-k) and shape-based behavior (plateau time, late discovery share) instead of single-run time-to-first-bug.

#### Bugs found at fixed time budgets (median [IQR])
| Fuzzer | Runs | 1h | 4h | 8h | 24h |
|---|---|---|---|---|---|
| medusa | 4 | 4 [4,4] | 4 [4,5] | 5 [4,6] | 5 [4,6] |
| echidna | 4 | 2 [2,2] | 3 [3,3] | 3 [3,3] | 3 [3,3] |
| foundry | 4 | 0 [0,0] | 0 [0,0] | 0 [0,0] | 0 [0,0] |

#### Overall metrics
| Fuzzer | AUC (norm) | Plateau time | Late discovery share | Final median | Final IQR |
|---|---|---|---|---|---|
| medusa | 0.755 | 4.30h | 0.000 | 5 | 2.00 |
| echidna | 0.467 | 2.50h | 0.000 | 3 | 0.25 |
| foundry | 0.000 | 0.00h | 0.000 | 0 | 0.00 |

#### Milestones: time-to-k and success rates
| Fuzzer | time-to-1 (p50) | time-to-3 (p50) | time-to-5 (p50) | reach-1 rate | reach-3 rate | reach-5 rate |
|---|---|---|---|---|---|---|
| medusa | 0.10h | 0.95h | 2.25h | 100.0% | 100.0% | 50.0% |
| echidna | 0.10h | 2.20h | inf | 100.0% | 100.0% | 0.0% |
| foundry | inf | inf | inf | 0.0% | 0.0% | 0.0% |

#### Shape-based interpretation (rules of thumb)
- **Fast-start / early-plateau**: high early checkpoint median + early plateau time + low late discovery share.
- **Steady**: moderate AUC, later plateau, consistent improvements across checkpoints, moderate variance.
- **Slow-burn / late-surge**: low early checkpoints but high late discovery share and later plateau time; often higher final median.

#### Limitations
- This dataset does **not** identify which specific bugs were found. It measures only counts.
- Bug depth/complexity cannot be measured directly without per-bug metadata (e.g., call-sequence length, coverage, or state metrics).
- Harness design still affects results; mitigate by keeping harness identical across fuzzers and reporting many runs.

## Broken invariants

- Budget filter: **12.00h**
- Events considered: **33 / 33**
- Unique invariants: **10**

#### Per-fuzzer totals

| Fuzzer | Invariants |
|---|---:|
| echidna | 4 |
| medusa | 6 |

#### High-level overlap

- Shared by all fuzzers: **0**
- Exclusive to `echidna`: **4**
- Exclusive to `medusa`: **6**

#### Grouped invariants

<details>
<summary>Exclusive to <code>echidna</code> (4)</summary>

- `doomsday_depositWithdrawSymmetrical(uint256)`
- `doomsday_mintRedeemSymmetrical(uint256)`
- `property_accumulatorSharesDecreaseOnFulfill_exact()`
- `property_previewEquivalenceFromShares(uint256)`

</details>

<details>
<summary>Exclusive to <code>medusa</code> (6)</summary>

- `CryticTester.doomsday_depositWithdrawSymmetrical(uint256)`
- `CryticTester.doomsday_mintRedeemSymmetrical(uint256)`
- `CryticTester.property_accumulatorSharesDecreaseOnFulfill_exact()`
- `CryticTester.property_comparePreviewMintAndConvertToAssets(uint256)`
- `CryticTester.property_previewEquivalenceFromAssets(uint256)`
- `CryticTester.property_previewEquivalenceFromShares(uint256)`

</details>

<details>
<summary>Shared by all fuzzers (0)</summary>

_None._

</details>

## Manifest

- scfuzzbench_commit: `87c113ca7dc77c2b3ef6df418f761b010f6cd835`
- target_repo_url: [https://github.com/Recon-Fuzz/superform-v2-periphery-scfuzzbench](https://github.com/Recon-Fuzz/superform-v2-periphery-scfuzzbench)
- target_commit: `dev-recon`
- benchmark_type: `property`
- instance_type: `c6a.4xlarge`
- instances_per_fuzzer: `4`
- timeout_hours: `12`
- aws_region: `us-east-1`
- ubuntu_ami_id: `ami-0071174ad8cbb9e17`
- foundry_version: `v1.6.0-rc1`
- foundry_git_repo: `https://github.com/aviggiano/foundry`
- foundry_git_ref: `master`
- echidna_version: `2.3.1`
- medusa_version: `1.4.1`
- fuzzer_keys: `echidna, foundry, medusa`

## Artifacts

- Manifest (index): https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/runs/1771615839/027e45da412aec12841579d664c97ff8/manifest.json
- Report prefix: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/
- Analysis bundle: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/bundles/analysis.zip
- Logs bundle: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/bundles/logs.zip
- Corpus bundle: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/analysis/027e45da412aec12841579d664c97ff8/1771615839/bundles/corpus.zip
- Raw logs prefix: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/logs/1771615839/027e45da412aec12841579d664c97ff8/
- Raw corpus prefix: https://scfuzzbench-logs-185f44d6.s3.us-east-1.amazonaws.com/corpus/1771615839/027e45da412aec12841579d664c97ff8/
- Broken invariants (Markdown): `/data/generated/broken_invariants-1771615839-027e45da412aec12841579d664c97ff8.md`
- Broken invariants (CSV): `/data/generated/broken_invariants-1771615839-027e45da412aec12841579d664c97ff8.csv`
