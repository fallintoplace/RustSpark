import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from bench_experiment import benchmark, load_experiment, sqlite_reference, run_variant


def configuration(flags=None):
    return {
        "name": "count_rows", "sql": "SELECT COUNT(*) AS n FROM data",
        "rows": 8,
        "variants": [
            {"name": "default", "flags": []},
            {"name": "changed", "flags": flags if flags is not None else ["--batch-size", "1024"]},
        ],
    }


class ExperimentTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as folder:
            file = Path(folder) / "experiment.json"
            file.write_text(json.dumps(value), encoding="utf-8")
            return load_experiment(file)

    def test_shipped_scenarios_have_valid_sql_and_variants(self):
        directory = Path(__file__).resolve().parents[1] / "benchmarks" / "perf_experiments"
        paths = sorted(directory.glob("*.json"))
        self.assertTrue(paths, "At least one shipped performance experiment is required")
        seen = set()
        for path in paths:
            case = load_experiment(path)
            self.assertNotIn(case["name"], seen)
            seen.add(case["name"])
            digest, values = sqlite_reference(case["sql"], min(case["rows"], 128))
            self.assertEqual(len(digest), 64)
            self.assertIsInstance(values, list)

    def test_accepts_valid_configuration(self):
        self.assertEqual(self.load(configuration())["rows"], 8)

    def test_rejects_unknown_or_duplicate_flags(self):
        for flags in (
            ["--bad", "1"], ["--batch-size", "0"],
            ["--batch-size", "1", "--batch-size", "2"], ["--batch-size"],
            ["--target-partitions", "-1"],
        ):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                self.load(configuration(flags))

    def test_rejects_duplicates_and_write_statements(self):
        cases = [
            {"name": "not safe", **{k: v for k, v in configuration().items() if k != "name"}},
            {**configuration(), "sql": "DROP TABLE data"},
            {**configuration(), "rows": 0},
            {**configuration(), "variants": [{"name": "only", "flags": []}]},
            {**configuration(), "variants": [{"name": "same", "flags": []}] * 2},
        ]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.load(case)

    def test_sqlite_oracle_reads_real_fixture_values(self):
        signature, rows = sqlite_reference(
            "SELECT city, SUM(amount) AS total FROM data GROUP BY city", 64
        )
        self.assertEqual(len(signature), 64)
        self.assertEqual(len(rows), 16)
        self.assertEqual(sqlite_reference("SELECT COUNT(*) AS n FROM data", 8)[1], [{"n": 8}])

    def test_runner_verifies_digest_and_ignores_warmup(self):
        expected, values = sqlite_reference("SELECT COUNT(*) AS n FROM data", 8)
        output = mock.Mock(stdout='{"n": 8}\n')
        with mock.patch("bench_experiment.subprocess.run", return_value=output) as runner:
            result = run_variant("fake", Path("unused"), "SELECT COUNT(*) AS n FROM data",
                                 [], expected, len(values), 2, 1, 5)
        self.assertEqual(runner.call_count, 3)
        self.assertEqual(len(result["samples_ms"]), 2)
        with mock.patch("bench_experiment.subprocess.run",
                        return_value=mock.Mock(stdout='{"n": 9}\n')):
            with self.assertRaisesRegex(ValueError, "differs"):
                run_variant("fake", Path("unused"), "SELECT COUNT(*) AS n FROM data",
                            [], expected, len(values), 1, 0, 5)

    def test_ordered_results_reject_permutations(self):
        sql = "SELECT id FROM data ORDER BY id LIMIT 2"
        expected, values = sqlite_reference(sql, 8)
        wrong_order = mock.Mock(stdout='{"id": 1}\n{"id": 0}\n')
        with mock.patch("bench_experiment.subprocess.run", return_value=wrong_order):
            with self.assertRaisesRegex(ValueError, "differs"):
                run_variant("fake", Path("unused"), sql, [],
                            expected, len(values), 1, 0, 5, ordered_values=values)

    def test_ordered_configuration_is_boolean(self):
        invalid = {**configuration(), "ordered": "yes"}
        with self.assertRaises(ValueError):
            self.load(invalid)

    def test_report_contains_two_verified_variants(self):
        with mock.patch("bench_experiment.subprocess.run",
                        return_value=mock.Mock(stdout='{"n": 8}\n')):
            report = benchmark("fake", configuration(), repeat=1, warmup=0, timeout=5)
        self.assertEqual([r["name"] for r in report["results"]], ["default", "changed"])
        self.assertEqual(report["results"][0]["result_rows"], 1)


if __name__ == "__main__":
    unittest.main()
