import tempfile
import unittest
from pathlib import Path

from analysis import analyze


class ThroughputParserTests(unittest.TestCase):
    def write_log(self, lines):
        tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        try:
            tmp.write("\n".join(lines) + "\n")
            tmp.close()
            return Path(tmp.name)
        except Exception:
            tmp.close()
            raise

    def test_parses_medusa_txps_and_gasps_from_actual_status_line(self):
        log_path = self.write_log(
            [
                "fuzz: elapsed: 3s, calls: 30779 (10258/sec), seq/s: 127, branches hit: 507, corpus: 75, failures: 8/382, gas/s: 2425494111",
            ]
        )

        samples = analyze.parse_throughput_log(log_path, "run-1", "i-1", "medusa-vtest")
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].fuzzer, "medusa")
        self.assertEqual(samples[0].source, "text-rate")
        self.assertAlmostEqual(samples[0].elapsed_seconds, 3.0)
        self.assertAlmostEqual(samples[0].tx_per_second, 10258.0)
        self.assertAlmostEqual(samples[0].gas_per_second, 2425494111.0)

    def test_parses_echidna_gasps_and_derives_txps_from_actual_status_lines(self):
        log_path = self.write_log(
            [
                "[2026-02-04 13:27:56.81] [status] tests: 2/11, fuzzing: 11116/50000, values: [], cov: 4245, corpus: 6, shrinking: W0:3107/5000(4), gas/s: 22556177076",
                "[2026-02-04 13:27:59.82] [status] tests: 2/11, fuzzing: 31812/50000, values: [], cov: 4245, corpus: 6, gas/s: 41571157176",
            ]
        )

        samples = analyze.parse_throughput_log(log_path, "run-1", "i-1", "echidna-vtest")
        self.assertEqual(len(samples), 2)
        self.assertEqual(samples[0].fuzzer, "echidna")
        self.assertIsNone(samples[0].tx_per_second)
        self.assertAlmostEqual(samples[0].gas_per_second, 22556177076.0)
        self.assertEqual(samples[1].source, "text-cumulative")
        self.assertAlmostEqual(samples[1].elapsed_seconds, 3.01)
        self.assertAlmostEqual(samples[1].tx_per_second, 31812.0 / 3.01, places=4)
        self.assertAlmostEqual(samples[1].gas_per_second, 41571157176.0)

    def test_foundry_actual_invariant_lines_do_not_emit_throughput(self):
        log_path = self.write_log(
            [
                '{"type":"invariant_failure","timestamp":1771871209,"invariant":"invariant_number_change_requires_sequence","failed_total":1}',
                '{"type":"invariant_metrics","timestamp":1771871209,"invariant":"invariant_number_change_requires_sequence","failed_current":1,"failed_total":1,"metrics":{"cumulative_edges_seen":15,"cumulative_features_seen":0,"corpus_count":0,"favored_items":0}}',
                "invariant_counter_increment() (runs: 40, calls: 4000, reverts: 519)",
            ]
        )

        samples = analyze.parse_throughput_log(log_path, "run-1", "i-1", "foundry-git-test")
        self.assertEqual(samples, [])


if __name__ == "__main__":
    unittest.main()
