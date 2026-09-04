"""Benchmarking one fuzzer twice, at two revisions, as a fuzzer variant."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from analysis import analyze, events_to_cumulative

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.benchmark_run_state import validate_fuzzer_variants  # noqa: E402


def load_generate_docs_site():
    script_path = REPO_ROOT / "scripts" / "generate_docs_site.py"
    spec = importlib.util.spec_from_file_location("generate_docs_site", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load spec for {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SeriesMapTests(unittest.TestCase):
    def test_a_fuzzer_with_two_labels_is_split_per_label(self):
        series_map = analyze.build_series_map(
            ["echidna-v2.3.1", "echidna-2-2-6", "medusa-v1.5.1"]
        )

        self.assertEqual(series_map["echidna-v2.3.1"], "echidna-v2.3.1")
        self.assertEqual(series_map["echidna-2-2-6"], "echidna-2-2-6")
        self.assertEqual(series_map["medusa-v1.5.1"], "medusa")

    def test_single_revision_runs_keep_the_plain_fuzzer_name(self):
        series_map = analyze.build_series_map(
            ["echidna-v2.3.1", "medusa-v1.5.1", "foundry-git-abc1234", "recon-v0.4.18"]
        )

        self.assertEqual(
            sorted(series_map.values()),
            ["echidna", "foundry", "medusa", "recon-fuzzer"],
        )

    def test_raw_labels_splits_every_fuzzer(self):
        labels = ["echidna-v2.3.1", "medusa-v1.5.1"]

        self.assertEqual(
            analyze.build_series_map(labels, raw_labels=True),
            {label: label for label in labels},
        )

    def test_unknown_label_falls_back_to_the_normalized_name(self):
        self.assertEqual(analyze.series_for_label("medusa-v1.5.1", {}), "medusa")

    def test_apply_series_map_rewrites_records(self):
        event = analyze.Event(
            run_id="run-1",
            instance_id="i-1",
            fuzzer="echidna",
            fuzzer_label="echidna-2-2-6",
            event="assertion",
            elapsed_seconds=1.0,
            source="log",
            log_path="echidna.log",
        )

        [applied] = analyze.apply_series_map(
            [event], analyze.build_series_map(["echidna-v2.3.1", "echidna-2-2-6"])
        )

        self.assertEqual(applied.fuzzer, "echidna-2-2-6")
        self.assertEqual(applied.fuzzer_label, "echidna-2-2-6")


def write_logs_dir(root: Path, instances: dict[str, str]) -> Path:
    logs_dir = root / "analysis"
    for instance_label, contents in instances.items():
        instance_dir = logs_dir / instance_label
        instance_dir.mkdir(parents=True)
        (instance_dir / "fuzzer.log").write_text(contents, encoding="utf-8")
    return logs_dir


TWO_REVISION_LOGS = {
    "i-aaaa1111-echidna-v2.3.1": (
        "[2026-03-01 00:00:00] starting\n"
        "[2026-03-01 00:00:10] Test echidna_solvency() falsified!\n"
    ),
    "i-bbbb2222-echidna-2-2-6": (
        "[2026-03-01 00:00:00] starting\n"
        "[2026-03-01 00:00:20] Test echidna_liquidity() falsified!\n"
    ),
    "i-cccc3333-medusa-v1.5.1": "[2026-03-01 00:00:00] no findings here\n",
}


def run_analysis(logs_dir: Path, out_dir: Path, *extra: str) -> None:
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_analysis_filtered.py"),
            "--logs-dir",
            str(logs_dir),
            "--out-dir",
            str(out_dir),
            "--run-id",
            "1700000000",
            *extra,
        ],
        check=True,
        capture_output=True,
    )


def event_series(out_dir: Path) -> set[str]:
    rows = (out_dir / "events.csv").read_text(encoding="utf-8").splitlines()
    header = rows[0].split(",")
    fuzzer_column = header.index("fuzzer")
    return {row.split(",")[fuzzer_column] for row in rows[1:] if row}


class TwoRevisionAnalysisTests(unittest.TestCase):
    def test_two_revisions_of_one_fuzzer_stay_apart(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            logs_dir = write_logs_dir(root, TWO_REVISION_LOGS)
            out_dir = root / "data"

            run_analysis(logs_dir, out_dir)

            self.assertEqual(
                event_series(out_dir), {"echidna-v2.3.1", "echidna-2-2-6"}
            )

    def test_excluding_one_revision_restores_the_plain_fuzzer_name(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            logs_dir = write_logs_dir(root, TWO_REVISION_LOGS)
            out_dir = root / "data"

            run_analysis(logs_dir, out_dir, "--exclude-fuzzers", "echidna-2-2-6")

            self.assertEqual(event_series(out_dir), {"echidna"})


class CumulativeSeriesTests(unittest.TestCase):
    def test_runs_without_events_join_their_own_revision(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            logs_dir = write_logs_dir(
                Path(tmp_dir),
                {
                    "i-aaaa1111-echidna-v2.3.1": "",
                    "i-bbbb2222-echidna-2-2-6": "",
                },
            )
            events = [
                {
                    "run_id": "1700000000",
                    "instance_id": "i-aaaa1111",
                    "fuzzer": "echidna-v2.3.1",
                    "fuzzer_label": "echidna-v2.3.1",
                    "elapsed_seconds": "60",
                }
            ]

            rows = events_to_cumulative.build_cumulative_rows(
                events,
                include_zero=True,
                logs_dir=logs_dir,
                run_id="1700000000",
            )

        by_series: dict[str, list] = {}
        for fuzzer, run_key, _hours, bugs in rows:
            by_series.setdefault(fuzzer, []).append((run_key, bugs))

        self.assertEqual(sorted(by_series), ["echidna-2-2-6", "echidna-v2.3.1"])
        # The revision that found nothing still contributes its zero-bug run.
        self.assertEqual(by_series["echidna-2-2-6"], [("1700000000:i-bbbb2222", 0)])
        self.assertEqual(
            by_series["echidna-v2.3.1"],
            [("1700000000:i-aaaa1111", 0), ("1700000000:i-aaaa1111", 1)],
        )


class VariantRequestValidationTests(unittest.TestCase):
    def test_accepts_a_second_revision_of_a_built_in_fuzzer(self):
        self.assertEqual(
            validate_fuzzer_variants(
                [{"key": "echidna-2-2-6", "base": "echidna", "version": "2.2.6"}]
            ),
            [
                {
                    "key": "echidna-2-2-6",
                    "base": "echidna",
                    "version": "2.2.6",
                    "ci": {},
                    "source": {},
                    "env": {},
                }
            ],
        )

    def test_key_must_be_prefixed_with_its_base_fuzzer(self):
        with self.assertRaisesRegex(ValueError, "must start with 'echidna-'"):
            validate_fuzzer_variants([{"key": "old-echidna", "base": "echidna"}])

    def test_base_must_be_a_supported_fuzzer(self):
        with self.assertRaisesRegex(ValueError, "not a supported fuzzer"):
            validate_fuzzer_variants([{"key": "nope-1", "base": "nope"}])

    def test_key_may_not_shadow_a_built_in_fuzzer(self):
        with self.assertRaisesRegex(ValueError, "must start with"):
            validate_fuzzer_variants([{"key": "echidna", "base": "echidna"}])

    def test_duplicate_keys_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate fuzzer variant key"):
            validate_fuzzer_variants(
                [
                    {"key": "echidna-a", "base": "echidna"},
                    {"key": "echidna-a", "base": "echidna"},
                ]
            )

    def test_version_is_pinned_by_its_own_field_not_by_env(self):
        with self.assertRaisesRegex(ValueError, "may not override ECHIDNA_VERSION"):
            validate_fuzzer_variants(
                [
                    {
                        "key": "echidna-2-2-6",
                        "base": "echidna",
                        "env": {"ECHIDNA_VERSION": "2.2.6"},
                    }
                ]
            )

    def test_version_rejects_shell_metacharacters(self):
        with self.assertRaisesRegex(ValueError, "invalid version"):
            validate_fuzzer_variants(
                [{"key": "echidna-x", "base": "echidna", "version": "2.2.6 && id"}]
            )

    def test_unknown_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported field"):
            validate_fuzzer_variants(
                [{"key": "echidna-x", "base": "echidna", "install_path": "/etc/passwd"}]
            )

    def test_variant_env_accepts_the_same_keys_as_fuzzer_env(self):
        [variant] = validate_fuzzer_variants(
            [
                {
                    "key": "echidna-w",
                    "base": "echidna",
                    "env": {"SCFUZZBENCH_WORKERS": "8"},
                }
            ]
        )

        self.assertEqual(variant["env"], {"SCFUZZBENCH_WORKERS": "8"})


class VariantCiBuildTests(unittest.TestCase):
    """A variant can pin its own CI build of Echidna, for master-vs-PR runs."""

    PR_CI = {
        "run_id": "33555211362",
        "artifact_name": "echidna-redistributable-x86_64-linux",
        "artifact_sha256": "c480d8599e643ee587cdb51fdffadb73a5291f46d97002398ab6ffadc55198b0",
        "commit": "55842ac2da34f40992cf48f211a0df1ede8e2fb9",
    }

    def test_accepts_a_ci_build_and_normalizes_case(self):
        [variant] = validate_fuzzer_variants(
            [
                {
                    "key": "echidna-pr-1614",
                    "base": "echidna",
                    "ci": dict(self.PR_CI, commit=self.PR_CI["commit"].upper()),
                }
            ]
        )

        self.assertEqual(variant["ci"], self.PR_CI)
        self.assertEqual(variant["version"], "")

    def test_ci_builds_are_echidna_only(self):
        with self.assertRaisesRegex(ValueError, "only for echidna"):
            validate_fuzzer_variants(
                [{"key": "medusa-x", "base": "medusa", "ci": self.PR_CI}]
            )

    def test_a_variant_pins_a_version_or_a_ci_build_not_both(self):
        with self.assertRaisesRegex(ValueError, "both a version and a CI build"):
            validate_fuzzer_variants(
                [
                    {
                        "key": "echidna-x",
                        "base": "echidna",
                        "version": "2.2.6",
                        "ci": self.PR_CI,
                    }
                ]
            )

    def test_partial_ci_inputs_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "missing: artifact_name"):
            validate_fuzzer_variants(
                [{"key": "echidna-x", "base": "echidna", "ci": {"run_id": "1"}}]
            )

    def test_artifact_must_be_a_linux_build(self):
        with self.assertRaisesRegex(ValueError, "Linux artifact"):
            validate_fuzzer_variants(
                [
                    {
                        "key": "echidna-x",
                        "base": "echidna",
                        "ci": dict(self.PR_CI, artifact_name="echidna-macos"),
                    }
                ]
            )

    def test_a_release_variant_opts_out_of_the_run_level_build(self):
        """Otherwise both sides would install the same bleeding-edge binary."""
        main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(encoding="utf-8")

        self.assertIn(
            "variant_release_keys = [\n"
            "    for variant in var.fuzzer_variants :\n"
            '    variant.key if variant.version != ""',
            main,
        )
        for resolved in ("instance_echidna_ci", "instance_medusa_source"):
            with self.subTest(resolved=resolved):
                block = main.split(f"  {resolved} = {{", 1)[1].split("\n  }", 1)[0]
                self.assertIn(
                    "contains(local.variant_release_keys, instance.fuzzer.key)", block
                )

    def test_preflight_verifies_every_variant_build(self):
        import importlib.util

        script = REPO_ROOT / "scripts" / "validate_bleeding_edge_tools.py"
        spec = importlib.util.spec_from_file_location("preflight", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        selected = module.parse_variant_ci(
            json.dumps(
                [
                    {"key": "echidna-pr-1614", "base": "echidna", "ci": self.PR_CI},
                    {"key": "echidna-release", "base": "echidna", "version": "2.2.6"},
                ]
            )
        )

        self.assertEqual([item["key"] for item in selected], ["echidna-pr-1614"])
        with self.assertRaises(module.ValidationError):
            module.parse_variant_ci(json.dumps([{"key": "x", "ci": {"run_id": "1"}}]))


class VariantSourceBuildTests(unittest.TestCase):
    """A medusa variant can pin its own source build, for master-vs-PR runs."""

    SOURCE = {
        "git_ref": "v1.4.1",
        "git_commit": "3857153837ab90ed73adc484414b4b43703a54fb",
    }

    def test_accepts_a_source_build_and_normalizes_case(self):
        [variant] = validate_fuzzer_variants(
            [
                {
                    "key": "medusa-v1-4-1",
                    "base": "medusa",
                    "source": dict(
                        self.SOURCE, git_commit=self.SOURCE["git_commit"].upper()
                    ),
                }
            ]
        )

        self.assertEqual(variant["source"], self.SOURCE)
        self.assertEqual(variant["version"], "")

    def test_source_builds_are_medusa_only(self):
        with self.assertRaisesRegex(ValueError, "only for medusa"):
            validate_fuzzer_variants(
                [{"key": "echidna-x", "base": "echidna", "source": self.SOURCE}]
            )

    def test_a_variant_pins_one_kind_of_build(self):
        with self.assertRaisesRegex(ValueError, "both a version and a source build"):
            validate_fuzzer_variants(
                [
                    {
                        "key": "medusa-x",
                        "base": "medusa",
                        "version": "1.4.1",
                        "source": self.SOURCE,
                    }
                ]
            )

    def test_partial_source_inputs_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "missing: git_commit"):
            validate_fuzzer_variants(
                [{"key": "medusa-x", "base": "medusa", "source": {"git_ref": "master"}}]
            )

    def test_commit_must_be_a_full_sha(self):
        with self.assertRaisesRegex(ValueError, "40-character SHA"):
            validate_fuzzer_variants(
                [
                    {
                        "key": "medusa-x",
                        "base": "medusa",
                        "source": dict(self.SOURCE, git_commit="v1.4.1"),
                    }
                ]
            )

    def test_preflight_verifies_every_variant_source(self):
        import importlib.util

        script = REPO_ROOT / "scripts" / "validate_bleeding_edge_tools.py"
        spec = importlib.util.spec_from_file_location("preflight_source", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        selected = module.parse_variant_source(
            json.dumps(
                [
                    {"key": "medusa-v1-4-1", "base": "medusa", "source": self.SOURCE},
                    {"key": "medusa-release", "base": "medusa", "version": "1.4.1"},
                ]
            )
        )

        self.assertEqual([item["key"] for item in selected], ["medusa-v1-4-1"])
        with self.assertRaises(module.ValidationError):
            module.parse_variant_source(
                json.dumps([{"key": "x", "source": {"git_ref": "master"}}])
            )

    def test_each_instance_resolves_its_own_medusa_source(self):
        main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(encoding="utf-8")

        self.assertIn(
            "git_ref    = local.variant_source_by_key[instance.fuzzer.key].git_ref",
            main,
        )
        self.assertIn(
            "git_commit = local.variant_source_by_key[instance.fuzzer.key].git_commit",
            main,
        )
        self.assertIn(
            "length(local.variant_source_keys) == 0 || local.medusa_source_enabled",
            main,
        )


class VariantProvisioningContractTests(unittest.TestCase):
    """A variant is a distinct identity running its base fuzzer's scripts."""

    def setUp(self):
        self.main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(
            encoding="utf-8"
        )
        self.template = (REPO_ROOT / "infrastructure" / "user_data.sh.tftpl").read_text(
            encoding="utf-8"
        )

    def test_bundled_scripts_are_selected_by_the_base_fuzzer(self):
        self.assertIn(
            "fuzzer_script_key_b64            = base64encode(instance.fuzzer.base)",
            self.main,
        )
        self.assertIn(
            'bash "/opt/scfuzzbench/fuzzers/$${fuzzer_script_key}/install.sh"',
            self.template,
        )
        self.assertIn(
            'bash "/opt/scfuzzbench/fuzzers/$${fuzzer_script_key}/run.sh" || true',
            self.template,
        )

    def test_instance_identity_stays_the_variant_key(self):
        self.assertIn(
            "decode_b64_env SCFUZZBENCH_FUZZER_KEY '${fuzzer_key_b64}'", self.template
        )

    def test_user_data_rejects_a_key_outside_its_base(self):
        self.assertIn(
            'if [[ "$${fuzzer_key}" != "$${fuzzer_script_key}" \\',
            self.template,
        )
        self.assertIn(
            '&& "$${fuzzer_key}" != "$${fuzzer_script_key}-"* ]]; then',
            self.template,
        )

    def test_environment_and_versions_are_resolved_per_fuzzer_key(self):
        self.assertIn(
            "for key, value in local.fuzzer_env_by_key[instance.fuzzer.key] : "
            "key => base64encode(value)",
            self.main,
        )
        self.assertIn(
            "lookup(local.variant_version_by_key, instance.fuzzer.key, version)",
            self.main,
        )

    def test_each_instance_resolves_its_own_echidna_build(self):
        self.assertIn(
            "echidna_ci_run_id_b64            = "
            "base64encode(local.instance_echidna_ci[instance_key].run_id)",
            self.main,
        )
        self.assertIn(
            "echidna_ci_commit_b64            = "
            "base64encode(local.instance_echidna_ci[instance_key].commit)",
            self.main,
        )
        # Repository and token stay run-level: one SSM parameter in the role.
        self.assertIn(
            'base64encode(instance.fuzzer.base == "echidna" ? var.echidna_ci_repo : "")',
            self.main,
        )
        self.assertIn(
            "length(local.variant_ci_keys) == 0 || local.echidna_ci_enabled", self.main
        )

    def test_unlisted_or_unknown_keys_fail_the_plan(self):
        self.assertIn("length(local.unknown_fuzzer_keys) == 0", self.main)
        self.assertIn("length(local.unscheduled_variant_keys) == 0", self.main)


class DocsSiteVariantTests(unittest.TestCase):
    def test_variant_shows_its_own_pinned_version(self):
        module = load_generate_docs_site()
        manifest = {
            "fuzzer_keys": ["echidna", "echidna-2-2-6", "medusa"],
            "echidna_version": "2.3.1",
            "medusa_version": "1.5.1",
            "fuzzer_variants": [
                {"key": "echidna-2-2-6", "base": "echidna", "version": "2.2.6"}
            ],
        }

        self.assertEqual(
            module.format_fuzzer_lines(manifest),
            ["`echidna (2.3.1)`", "`echidna-2-2-6 (2.2.6)`", "`medusa (1.5.1)`"],
        )

    def test_variant_without_a_version_shows_its_base_version(self):
        module = load_generate_docs_site()
        manifest = {
            "fuzzer_keys": ["medusa", "medusa-workers-8"],
            "medusa_version": "1.5.1",
            "fuzzer_variants": [
                {"key": "medusa-workers-8", "base": "medusa", "version": ""}
            ],
        }

        self.assertEqual(
            module.format_fuzzer_lines(manifest),
            ["`medusa (1.5.1)`", "`medusa-workers-8 (1.5.1)`"],
        )


class VariantRunStatePersistenceTests(unittest.TestCase):
    def test_recovery_inputs_may_not_persist_variant_env(self):
        from scripts.benchmark_run_state import validate_recovery_inputs

        payload = {
            "run_id": "1700000000",
            "terraform_backend_key": "runs/1700000000/terraform.tfstate",
            "existing_bucket_name": "bucket",
            "fuzzer_variants": [
                {"key": "echidna-x", "base": "echidna", "env": {"FOO": "bar"}}
            ],
        }

        with self.assertRaisesRegex(ValueError, "must not persist fuzzer variant env"):
            validate_recovery_inputs(
                payload,
                dict(payload, status="running"),
                run_id="1700000000",
                backend_key="runs/1700000000/terraform.tfstate",
                bucket="bucket",
            )

    def test_run_workflow_persists_variant_identity_without_env(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "benchmark-run.yml"
        ).read_text(encoding="utf-8")

        self.assertIn(
            '{"key": variant["key"], "base": variant["base"]}',
            workflow,
        )
        self.assertIn('-var "fuzzer_variants=${FUZZER_VARIANTS_JSON:-[]}"', workflow)


class VariantRequestSchemaTests(unittest.TestCase):
    def test_issue_template_offers_the_field(self):
        template = (
            REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "benchmark-request.md"
        ).read_text(encoding="utf-8")
        payload = json.loads(template.split("```json", 1)[1].split("```", 1)[0])

        self.assertEqual(payload["fuzzer_variants"], [])


if __name__ == "__main__":
    unittest.main()
