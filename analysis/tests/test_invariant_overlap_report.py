import csv
import tempfile
import unittest
from pathlib import Path

from analysis import invariant_overlap_report as overlap


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "events_overlap_fixture.csv"


class InvariantOverlapReportTests(unittest.TestCase):
    def build_result(self, budget_hours):
        events = overlap.load_events(FIXTURE)
        filtered = overlap.filter_budget(events, budget_hours)
        return overlap.build_overlap(filtered, total_events=len(events))

    def test_budget_filtering_and_exact_intersections(self):
        result = self.build_result(0.03)  # 108 seconds

        self.assertEqual(result.set_sizes, {"echidna": 4, "foundry": 4, "medusa": 4})
        self.assertEqual(result.intersections.get(("foundry",)), ["A"])
        self.assertEqual(result.intersections.get(("echidna",)), ["G"])
        self.assertEqual(result.intersections.get(("echidna", "medusa")), ["C"])
        self.assertEqual(result.intersections.get(("foundry", "medusa")), ["E"])
        self.assertEqual(result.intersections.get(("echidna", "foundry", "medusa")), ["B", "F"])
        self.assertNotIn("H", result.invariants)

    def test_unfiltered_includes_late_events(self):
        result = self.build_result(None)

        self.assertIn("H", result.invariants)
        self.assertEqual(result.intersections.get(("foundry", "medusa")), ["E", "H"])

    def test_first_seen_and_runs_hit(self):
        result = self.build_result(0.03)
        summary = result.invariants["A"]

        self.assertEqual(summary.fuzzers, ("foundry",))
        self.assertAlmostEqual(summary.first_seen_seconds["foundry"], 10.0)
        self.assertEqual(summary.runs_hit["foundry"], 2)

    def test_writes_artifacts_and_handles_no_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            out_csv = tmp_dir / "broken_invariants.csv"
            out_md = tmp_dir / "broken_invariants.md"
            out_png = tmp_dir / "invariant_overlap_upset.png"

            result = self.build_result(0.03)
            overlap.write_csv_report(result, out_csv)
            overlap.write_md_report(result, out_md, budget_hours=0.03, top_k=20)
            overlap.plot_upset(result, out_png, top_k=20)

            self.assertTrue(out_csv.exists())
            self.assertTrue(out_md.exists())
            self.assertTrue(out_png.exists())

            with out_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            row_a = next(row for row in rows if row["invariant"] == "A")
            self.assertEqual(row_a["foundry_runs_hit"], "2")

            empty_result = self.build_result(0.0)
            empty_md = tmp_dir / "empty_broken_invariants.md"
            empty_png = tmp_dir / "empty_invariant_overlap.png"
            overlap.write_md_report(empty_result, empty_md, budget_hours=0.0, top_k=20)
            overlap.plot_upset(empty_result, empty_png, top_k=20)

            self.assertIn("No broken invariants", empty_md.read_text(encoding="utf-8"))
            self.assertTrue(empty_png.exists())


if __name__ == "__main__":
    unittest.main()
