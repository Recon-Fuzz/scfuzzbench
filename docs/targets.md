# Targets

## Goal

This document is the playbook for adding a new benchmark target to the `Recon-Fuzz` ecosystem so it works in `scfuzzbench` with:

- `echidna`
- `medusa`
- `foundry` (Recon benchmark fork)

It includes lessons learned from:

- `aave-v4-scfuzzbench` (`v0.5.6-recon`)
- `superform-v2-periphery-scfuzzbench` (vulnerable commit + recon harness backport)

## Currently conforming targets

These targets currently conform to `scfuzzbench` expectations:

1. `aave-v4`:
   - https://github.com/Recon-Fuzz/aave-v4-scfuzzbench
2. `superform-v2-periphery`:
   - https://github.com/Recon-Fuzz/superform-v2-periphery-scfuzzbench
3. `scfuzzbench-canary`:
   - https://github.com/Recon-Fuzz/scfuzzbench-canary


## What "done" looks like

1. A public target repo exists in `Recon-Fuzz`, based on a vulnerable commit/tag/branch.
2. A base branch (usually `dev`) points to the vulnerable code snapshot.
3. A recon branch (usually `dev-recon`) adds the full recon harness and benchmark configs.
4. `dev-recon` compiles and can run local smoke trials for Echidna, Medusa, and Foundry.
5. PR `dev-recon -> dev` documents exact `/start` inputs for `scfuzzbench`.


## Non-negotiable constraints

1. Keep target code at the vulnerable point in time.
2. Bring recon harness from the branch/commit where invariants were introduced.
3. Copy the full recon setup, not partial files.
4. Local validation must be close to production runner settings.
5. Do not rely on target-specific quirks as global defaults in `scfuzzbench`.


## Initial sync workflow (expanded)

These were the original baseline instructions and still apply:

1. Go to `aave-v4-scfuzzbench`, checkout `v0.5.6-recon`, stash local changes, pull remote.
2. Go to `scfuzzbench`, checkout `main`, stash local changes, pull remote.
3. Diff `aave-v4-scfuzzbench` recon branch vs main to identify the Chimera setup under `test/recon/`.
4. Reproduce the same style for the new target.

Recommended command pattern:

```bash
git checkout <branch>
git stash push -u -m "wip before sync"
git pull --ff-only
```


## Step-by-step: add a new target repo

### 1) Create target repository and baseline branch

1. Create new repo under `Recon-Fuzz`, name pattern: `<project>-scfuzzbench`.
2. Clone upstream project.
3. Checkout the vulnerable commit.
4. Create `dev` branch at that commit.
5. Push `dev` and set it as default branch.

Example (Superform case):

- Upstream: `superform-xyz/v2-periphery`
- Vulnerable commit: `79c946332f72266ba0eeb7a3e062de371139e477`
- New repo: `Recon-Fuzz/superform-v2-periphery-scfuzzbench`


### 2) Create recon branch from baseline

1. Create `dev-recon` from `dev`.
2. Copy recon harness from the branch where invariants actually exist.
3. Copy configs used by fuzzers.

Minimum files to port:

1. `test/recon/` directory (full tree)
2. `foundry.toml`
3. `echidna.yaml`
4. `medusa.json`
5. Any required mock/helpers/remappings/scripts tied to recon tests


### 3) Ensure Foundry benchmark fork compatibility

`scfuzzbench` uses a Foundry fork (`aviggiano/foundry`, `master`) for benchmark runs.  
Your target `foundry.toml` must include benchmark-friendly invariant settings:

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

`continuous_run = true` is important for long-running benchmark behavior and metrics.


## Foundry assertion visibility hack (required)

Because of `foundry-rs/foundry#13322`, assertion-style failures in handler logic may not surface clearly in invariant results unless explicitly mapped.

Use the Aave pattern:

1. Prefix assertion reasons with `!!!`.
2. Use one explicit `invariant_assertion_failure_*` function per assertion.
3. Record assertion failures instead of immediately reverting inside overridden assert helpers.

Implementation pattern:

1. In `Properties.sol`, define constants:
   - `string constant ASSERTION_X = "!!! ...";`
2. In target handlers (`DoomsdayTargets`, `SuperVaultTargets`, etc), pass those constants to `t(...)`.
3. In `CryticToFoundry.sol`:
   - track failures: `mapping(string => bool) assertionFailures`
   - detect assertion labels via prefix `!!!`
   - override `gt/gte/lt/lte/eq/t` and record failures for assertion-tagged reasons
   - add one invariant checker per assertion constant
   - keep a no-op invariant (optional but useful): `invariant_noop()`

Also ensure `setUp()` routes fuzzing to handlers:

1. `targetContract(address(this))`
2. `targetSender(address(0x10000))`
3. `targetSender(address(0x20000))`
4. `targetSender(address(0x30000))`


## Tool-specific configuration rules

### Echidna

1. Target path must match repo layout.
2. Most Foundry repos use `test/`, not `tests/`.
3. If target file is `test/recon/CryticTester.sol`, use that exact path.

### Medusa

1. Do not use `compilation.platformConfig.target = "."` when forcing `solc` via crytic-compile.
2. Use a concrete file target, usually:
   - `test/recon/CryticTester.sol`
3. Ensure gas limits are high enough for this target's call graph.
4. If Medusa crashes with `insufficient gas for floor data gas cost`, increase:
   - `fuzzing.transactionGasLimit`
   - `fuzzing.blockGasLimit`

Example:

```json
"compilation": {
  "platform": "crytic-compile",
  "platformConfig": {
    "target": "test/recon/CryticTester.sol"
  }
}
```

Gas-limit example used to fix Superform:

```json
"blockGasLimit": 300000000,
"transactionGasLimit": 30000000
```


## Critical `scfuzzbench` default behavior

Global defaults must be generic and conform to standard Foundry layouts.

Current correct global default in `scfuzzbench/infrastructure/main.tf`:

- `ECHIDNA_TARGET = "test/recon/CryticTester.sol"`

Important:

1. Do not set `tests/recon/...` globally.
2. Use target-specific override only for non-conformant repos (legacy exceptions).

Target-specific override is done through `/start` -> `fuzzer_env_json`, not infra defaults.


## `/start` request guidance

For standard targets, keep `fuzzer_env_json` empty:

```json
"fuzzer_env_json": ""
```

Set only if needed for exceptional repos.

Typical fields:

1. `target_repo_url`: `https://github.com/Recon-Fuzz/<target-repo>`
2. `target_commit`: usually `dev-recon`
3. `benchmark_type`: `property` (or `optimization` if intended)
4. `instance_type`: e.g. `c6a.4xlarge`
5. `instances_per_fuzzer`: e.g. `2` for first validation
6. `timeout_hours`: e.g. `2` for validation run, larger for real benchmark
7. `fuzzers`: `["echidna","medusa","foundry"]`
8. Foundry fork fields:
   - `foundry_git_repo = "https://github.com/aviggiano/foundry"`
   - `foundry_git_ref = "master"`

For known non-conformant target only, example override:

```json
"fuzzer_env_json": "{\"ECHIDNA_TARGET\":\"tests/recon/CryticTester.sol\"}"
```


## Local preflight checklist (must pass before PR)

1. `forge test --match-contract CryticToFoundry --list`
2. Echidna compile/start smoke test
3. Medusa compile/start smoke test
4. Foundry invariant smoke test
5. 10-minute trials for all 3 fuzzers
6. `CryticToFoundry.sol` contains no `test_*` reproducer/unit-test functions

Suggested 10-minute commands:

```bash
# Echidna
timeout 600 echidna-test test/recon/CryticTester.sol --contract CryticTester --config echidna.yaml --format text

# Medusa
SOLC_VERSION=0.8.30 medusa fuzz --config medusa.json --timeout 600

# Foundry
timeout 600 forge test --match-contract CryticToFoundry --match-test 'invariant_' -vv
```

If Foundry prints only live metrics and no explicit fail summary, parse carefully and/or rerun with non-continuous mode for local debugging only:

```bash
FOUNDRY_INVARIANT_CONTINUOUS_RUN=false forge test --match-contract CryticToFoundry --match-test 'invariant_' -vv
```


## Production-parity testing notes

Local testing should mimic benchmark infra as closely as possible:

1. Same Foundry fork/revision (`aviggiano/master`)
2. Same Echidna and Medusa versions used in manifest
3. Same benchmark type
4. Same major config knobs (`continuous_run`, invariant depth/runs, corpus dirs)

This catches failures that only appear in benchmark runs.


## Common failure signatures and fixes

### Echidna error

`tests/recon/CryticTester.sol does not exist`

Fix:

1. Use `test/recon/CryticTester.sol` for standard Foundry targets.
2. Apply `tests/...` only per-target via `fuzzer_env_json` when truly needed.

### Medusa error

`. is a directory. Expected a Solidity file when not using a compilation framework.`

Fix:

1. Set `medusa.json` compilation target to concrete file:
   - `test/recon/CryticTester.sol`

### Medusa error

`insufficient gas for floor data gas cost: have X, want Y`

Fix:

1. Raise `transactionGasLimit` and `blockGasLimit` in `medusa.json`.
2. Re-run a short Medusa smoke trial and confirm it reaches fuzzing metrics and/or finds invariant failures.

### Foundry assertion failures not surfaced as expected

Fix:

1. Ensure reasons start with `!!!`
2. Ensure per-assertion `invariant_assertion_failure_*` functions exist
3. Ensure assert helpers are overridden and recording failures

### Foundry appears unrealistically fast and finds all bugs immediately

Likely cause:

1. `CryticToFoundry.sol` still contains hardcoded `test_*` reproducer/unit tests.
2. `scfuzzbench` runs `forge test --mc CryticToFoundry`, so those tests execute and bias benchmark results.

Fix:

1. Keep `CryticToFoundry` as an invariant harness only.
2. Remove all `test_*` reproducer functions from benchmark branches.
3. Verify with:
   - `forge test --match-contract CryticToFoundry --list`
   - ensure output contains only `invariant_*` (and optional `invariant_noop`).


## PR template requirements (`dev-recon -> dev`)

PR description should include:

1. Vulnerable baseline ref used for `dev`
2. Source of recon harness copied into `dev-recon`
3. Files copied/changed list (`test/recon`, configs, remappings)
4. Local smoke test results
5. 10-minute trial summary per fuzzer
6. Exact `/start` request JSON to run this benchmark in `scfuzzbench`
7. Any target-specific fuzzer overrides and why


## Lessons learned from Aave + Superform

1. Aave needed a non-standard `tests/recon` path; this must remain target-specific.
2. Superform highlighted why global defaults must stay generic (`test/recon`).
3. Medusa is sensitive to crytic compilation target when forcing `solc`; use explicit file target.
4. Foundry assertion visibility requires explicit shim + per-assertion invariants for reliable benchmark reporting.
5. Benchmark output showing zero bugs for a fuzzer can be a setup failure, not tool weakness. Always inspect raw logs.
6. Medusa can fail after successful compilation if gas caps are too low; watch for `floor data gas cost` errors.
7. Foundry benchmark targets must not include unit-style reproducers in `CryticToFoundry`; they invalidate performance/coverage comparability.


## Reference fixes applied during this effort

1. `scfuzzbench` global Echidna default path fix:
   - commit `2ff7461`
2. `superform-v2-periphery-scfuzzbench` Medusa target fix:
   - commit `7be8d5ec`
3. `superform-v2-periphery-scfuzzbench` Medusa gas-limit + Foundry harness cleanup:
   - commit `79cc7ba9`


## Agent invocation template (copy/paste)

Use this exact prompt format when delegating target onboarding to an agent:

```text
Read docs/targets.md and execute it end-to-end for the following target.

Inputs:
- upstream_target_repo_url: https://github.com/<org>/<repo>
- vulnerable_baseline_commit_sha_for_dev: <40-char sha>
- recon_harness_source_repo_url: https://github.com/<org>/<repo>
- recon_harness_source_ref_for_test_recon: <branch-or-commit>
- destination_repo_url: https://github.com/Recon-Fuzz/<new-target-repo>
- base_branch_name: dev
- recon_branch_name: dev-recon
- benchmark_type: property

Requirements:
1) Create/push base branch at vulnerable commit.
2) Create recon branch and port full recon harness + config files.
3) Ensure foundry.toml invariant settings are benchmark-compatible, including continuous_run=true.
4) Implement Foundry assertion visibility shim (`!!!` reasons + one invariant_assertion_failure_* per assertion) if missing.
5) Ensure Echidna target path and Medusa compilation target path are correct for this repo layout.
6) Run local smoke checks and 10-minute trials for echidna, medusa, foundry.
7) Open PR recon->base and include exact /start request JSON in PR description.

Deliverables:
- destination repo URL
- pushed branches and commit SHAs
- PR URL
- per-fuzzer 10-minute broken-invariant counts
- final /start JSON to run in scfuzzbench
```

Minimal quality gate before claiming done:

1. Target compiles with recon harness.
2. Echidna and Medusa both reach fuzzing stage (not compile/setup failure).
3. Foundry runs invariant campaign with benchmark-compatible config.
