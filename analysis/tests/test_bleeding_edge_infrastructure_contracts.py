import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class BleedingEdgeInfrastructureContractTests(unittest.TestCase):
    def test_echidna_token_access_uses_a_dedicated_exact_profile(self):
        main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(encoding="utf-8")

        self.assertIn('resource "aws_iam_role" "echidna_ci"', main)
        self.assertIn('resource "aws_iam_instance_profile" "echidna_ci"', main)
        self.assertIn('sid       = "ReadExactEchidnaToken"', main)
        self.assertIn("resources = [local.echidna_ci_token_ssm_parameter_arn]", main)
        self.assertIn('sid       = "DecryptExactEchidnaTokenKey"', main)
        self.assertIn('variable = "kms:EncryptionContext:PARAMETER_ARN"', main)
        self.assertIn('variable = "kms:ViaService"', main)
        self.assertNotIn(
            "compact([local.git_token_ssm_parameter_arn, local.echidna_ci_token_ssm_parameter_arn])",
            main,
        )
        self.assertIn(
            'each.value.fuzzer.base == "echidna" &&\n'
            '    local.instance_echidna_ci[each.key].run_id != ""',
            main,
        )

    def test_source_extractors_and_values_are_scoped_to_their_fuzzer(self):
        main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(encoding="utf-8")
        template = (REPO_ROOT / "infrastructure" / "user_data.sh.tftpl").read_text(
            encoding="utf-8"
        )

        # CI mode is resolved per instance. A release variant of Echidna must
        # not inherit the run-level repository or token and select CI mode.
        self.assertIn(
            'local.instance_echidna_ci[instance_key].run_id != "" '
            '? var.echidna_ci_repo : ""',
            main,
        )
        self.assertIn(
            'local.instance_echidna_ci[instance_key].run_id != "" '
            '? var.echidna_ci_token_ssm_parameter_name : ""',
            main,
        )
        self.assertNotIn(
            'instance.fuzzer.base == "echidna" ? var.echidna_ci_repo', main
        )
        # Medusa source inputs are resolved per instance so a variant that
        # pins a release opts out of the run-level source build.
        self.assertIn(
            'instance.fuzzer.base != "medusa" ||',
            main,
        )
        self.assertIn(
            "local.instance_medusa_source[instance_key].git_repo",
            main,
        )
        self.assertIn(
            "decode_b64_env ECHIDNA_CI_REPO '${echidna_ci_repo_b64}'",
            template,
        )
        self.assertIn(
            "decode_b64_env MEDUSA_GIT_REPO '${medusa_git_repo_b64}'",
            template,
        )

    def test_stable_defaults_are_blank_and_add_no_kms_permission(self):
        variables = (REPO_ROOT / "infrastructure" / "variables.tf").read_text(
            encoding="utf-8"
        )
        main = (REPO_ROOT / "infrastructure" / "main.tf").read_text(encoding="utf-8")

        for name in (
            "echidna_ci_repo",
            "echidna_ci_token_ssm_parameter_name",
            "echidna_ci_token_kms_key_arn",
            "medusa_git_repo",
        ):
            block = variables.split(f'variable "{name}"', 1)[1].split("variable ", 1)[0]
            self.assertIn('default     = ""', block)
        self.assertIn(
            'for_each = var.echidna_ci_token_kms_key_arn == "" ? []',
            main,
        )


if __name__ == "__main__":
    unittest.main()
