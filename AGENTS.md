# AGENTS.md

Instructions for working on Terraform in this repository.

## Scope
- Applies to any Terraform (`.tf`, `.tfvars`, `.hcl`) changes in this repo.

## Workflow
- Prefer small, focused changes; avoid unrelated refactors.
- Keep Terraform and module versions stable unless explicitly asked to upgrade.
- Use variables/locals instead of hard-coding environment-specific values.
- Keep provider configuration centralized; avoid duplicating provider blocks across modules.

## Formatting & Validation
- Run `terraform fmt -recursive` on touched Terraform files.
- Run `terraform validate` when possible (requires initialized working dir).
- If validation can't run (missing credentials/backends), note it in the response.
- Prefer the Makefile helpers when working locally:
  - `make terraform-fmt`
  - `make terraform-validate`
  - `make terraform-plan`
  - `make terraform-init-backend` (when using a remote backend)
  - `make terraform-deploy`
  - `make terraform-destroy`

## Files & Structure
- Keep module boundaries clean; do not create cross-module dependencies.
- Keep outputs minimal and documented; avoid leaking secrets via outputs.
- Prefer `locals` for derived values and `data` sources over hard-coded IDs.

## Safety & Secrets
- Never commit credentials, tokens, or private keys.
- Use environment variables or secret managers for sensitive values.
- If a change could affect state/backends, call it out explicitly.

## Tests / Checks
- If the repo has CI or linting for Terraform, follow it.
- If adding new resources, include appropriate tags/labels per existing patterns.

## Analysis workflow
- Use the Makefile to generate all analysis outputs in one pass:
  - `make results-analyze-all BUCKET=... RUN_ID=... BENCHMARK_UUID=...`
- Optional overrides:
  - `EXCLUDE_FUZZERS=...` (comma-separated), `DURATION_HOURS=...`
  - `REPORT_BUDGET=...`, `REPORT_GRID_STEP_MIN=...`, `REPORT_CHECKPOINTS=...`, `REPORT_KS=...`
