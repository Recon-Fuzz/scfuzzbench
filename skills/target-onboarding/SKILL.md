---
name: target-onboarding
description: Create and execute onboarding for a new scfuzzbench benchmark target end-to-end, including target repo setup, validation, and PR with /start payload.
metadata:
  short-description: Onboard a benchmark target end-to-end
---

# Target Onboarding Skill

Use this skill when onboarding a new benchmark target for `Recon-Fuzz/scfuzzbench`.

This skill covers:
- creating/maintaining `dev` and `dev-recon` branches in the target repo
- porting recon harness/config files
- running local validation
- opening `dev-recon -> dev` PR with exact `/start` request JSON

## Inputs

Required:
- `upstream_target_repo_url`: upstream project URL
- `vulnerable_baseline_commit_sha_for_dev`: baseline commit for `dev`
- `recon_harness_source_repo_url`: source repo containing recon harness
- `recon_harness_source_ref_for_test_recon`: source branch/commit to copy harness from
- `destination_repo_url`: `https://github.com/Recon-Fuzz/<repo>-scfuzzbench`
- `base_branch_name`: usually `dev`
- `recon_branch_name`: usually `dev-recon`
- `benchmark_type`: `property` or `optimization`

Optional:
- requester notes and constraints

## Non-negotiable constraints

1. Keep target code at the vulnerable point in time.
2. Port the full recon harness, not partial files.
3. Validate locally before opening PR.
4. Keep global defaults in `scfuzzbench` generic; use per-target overrides only when needed.
5. Do not leak secrets in issues/PRs.
6. Every benchmark target must include canary checks:
   - one canary assertion failure
   - one canary global invariant failure prefixed with `invariant_`

## Workflow

### 1) Create target repo baseline branch

In destination repo:
1. Checkout vulnerable baseline commit.
2. Create `base_branch_name` (default `dev`) at that commit.
3. Push and set as baseline/default as needed.

### 2) Create recon branch and port harness

1. Create `recon_branch_name` from base branch (default `dev-recon` from `dev`).
2. Port full recon setup from source ref.

Minimum files/directories to port:
1. `test/recon/` (full tree)
2. `foundry.toml`
3. `echidna.yaml`
4. `medusa.json`
5. Required helpers/remappings/scripts used by recon tests

### 3) Ensure benchmark-compatible config

`foundry.toml` invariant section must include benchmark-compatible values:

```toml
[invariant]
runs = 500000000
depth = 100
include_storage = true
show_solidity = true
show_metrics = true
fail_on_revert = false
continuous_run = true
corpus_dir = "corpus/foundry"
```

### 4) Foundry assertion visibility shim

Because assertion failures can be hidden in invariant output, enforce:
1. assertion reason strings prefixed with `!!!`
2. per-assertion `invariant_assertion_failure_*` checks
3. overridden assert helpers (`gt/gte/lt/lte/eq/t`) that record assertion failures
4. `setUp()` with handler routing (`targetContract`, multiple `targetSender` values)
5. include `invariant_assertion_failure_CANARY_ASSERTION_FAILURE` that fails immediately in canary runs

### 5) Canary requirement for every target

Add these canaries to each target harness:
1. Assertion canary:
   - assertion reason string: `!!! CANARY_ASSERTION_FAILURE`
   - Foundry wrapper invariant: `invariant_assertion_failure_CANARY_ASSERTION_FAILURE`
2. Global invariant canary:
   - invariant function name must start with `invariant_`
   - use `invariant_canary_global_invariant_failure` and make it fail immediately

Both canaries are intentional failures used to verify:
1. all fuzzers emit failures on the target
2. the analysis/parser pipeline is capturing failures correctly

### 6) Fuzzer-specific path rules

Echidna:
1. usually use `test/recon/CryticTester.sol`
2. use `tests/...` only for target-specific exceptions
3. use assertion-mode config that matches recon harness invariants:
   - `testMode: "assertion"`
   - `prefix: "invariant_"`
   - make sure Echidna and Medusa global invariants are prefixed with `invariant_` instead of `property_` or `echidna_`

Medusa:
1. use concrete compilation target file (not `"."`)
2. usually `test/recon/CryticTester.sol`
3. if gas-floor errors occur, raise gas limits

Example:

```json
"compilation": {
  "platform": "crytic-compile",
  "platformConfig": {
    "target": "test/recon/CryticTester.sol"
  }
}
```

### 7) Local validation before PR

Run all:
1. `forge test --match-contract CryticToFoundry --list`
2. Echidna smoke run
3. Medusa smoke run
4. Foundry invariant smoke run
5. 10-minute trial for each fuzzer
6. Ensure `CryticToFoundry.sol` has no `test_*` repro/unit tests
7. Canary smoke checks must fail immediately:
   - `FOUNDRY_INVARIANT_CONTINUOUS_RUN=false forge test --match-contract CryticToFoundry --match-test invariant_canary_global_invariant_failure -vv`
   - `FOUNDRY_INVARIANT_CONTINUOUS_RUN=false forge test --match-contract CryticToFoundry --match-test invariant_assertion_failure_CANARY_ASSERTION_FAILURE -vv`

Suggested 10-minute commands:

```bash
# Echidna
timeout 600 echidna test/recon/CryticTester.sol --contract CryticTester --config echidna.yaml --format text

# Medusa
SOLC_VERSION=0.8.30 medusa fuzz --config medusa.json --timeout 600

# Foundry
timeout 600 forge test --match-contract CryticToFoundry --match-test 'invariant_' -vv
```

Debug-only fallback for Foundry output inspection:

```bash
FOUNDRY_INVARIANT_CONTINUOUS_RUN=false forge test --match-contract CryticToFoundry --match-test 'invariant_' -vv
```

### 8) Open PR from recon branch to base branch

Create PR `dev-recon -> dev` (or configured branch names).

PR description must include:
1. vulnerable baseline ref used for base branch
2. recon harness source ref
3. files copied/changed
4. local smoke test summary
5. 10-minute trial summary per fuzzer
6. canary validation summary (assertion canary + global invariant canary)
7. exact `/start` request JSON for `scfuzzbench`
8. any target-specific overrides and why

### 9) Final `/start` request JSON guidance

Typical fields:
1. `target_repo_url`: destination repo URL
2. `target_commit`: usually `dev-recon`
3. `benchmark_type`: `property` or `optimization`
4. `instance_type`
5. `instances_per_fuzzer`
6. `timeout_hours`
7. `fuzzers`: `["echidna","medusa","foundry"]`
8. optional `fuzzer_env_json` only when target-specific override is necessary
9. optional Foundry source fields:
   - `foundry_git_repo`: `https://github.com/aviggiano/foundry`
   - `foundry_git_ref`: `master`

## Common failures and fixes

1. Echidna: `tests/recon/CryticTester.sol does not exist`
   - fix target path to `test/recon/CryticTester.sol` unless repo is a known exception
2. Medusa: target `"."` treated as directory
   - use explicit Solidity file target
3. Medusa: `insufficient gas for floor data gas cost`
   - raise `transactionGasLimit` and `blockGasLimit`
4. Foundry failures not surfaced
   - verify `!!!` prefix + per-assertion invariants + overridden assert helpers
5. Foundry unrealistically fast/all bugs immediate
   - remove any `test_*` functions in `CryticToFoundry`
6. Echidna returns 0 issues unexpectedly
   - enforce `testMode: "assertion"` with `prefix: "invariant_"` and avoid `prefix: "property_"` or `prefix: "echidna_"`

## Completion checklist

Done means all are true:
1. destination repo is created/updated in `Recon-Fuzz`
2. base and recon branches are pushed
3. recon PR is open with required validation details
4. canary assertion + canary `invariant_` global failure are present and intentionally failing
5. exact `/start` JSON is provided
6. PR URL is recorded in final report; include tracking issue URL only if one was explicitly requested
