import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import bench_compare


class ComparisonTests(unittest.TestCase):
    def test_commands_use_shared_input_and_sql(self):
        csv = pathlib.Path("/tmp/with ' quote.csv")
        query = "SELECT 1"
        for engine in ["rustspark", "duckdb", "spark"]:
            command = bench_compare.command_for(engine, "/bin/rustspark", csv, query)
            self.assertIn(str(csv), command)
            self.assertIn(query, command)

    def test_unknown_engine(self):
        with self.assertRaises(ValueError):
            bench_compare.command_for("unknown", "binary", pathlib.Path("x"), "sql")

    def test_correctness_check_precedes_timing(self):
        with mock.patch.object(bench_compare, "execute",
                               return_value=(1.0, "wrong", [{"n": 0}])):
            with self.assertRaisesRegex(RuntimeError, "Incorrect result"):
                bench_compare.benchmark("fake", ["rustspark"], 2, 1, 0, 1)

    def test_validated_repeated_samples(self):
        from bench_rustspark import expected_results, output_signature
        def fake_execute(command, timeout):
            query = command[command.index("--sql") + 1]
            name = next(k for k, v in bench_compare.WORKLOADS.items() if v == query)
            import json
            rows = expected_results(2)[name]
            digest = output_signature("\n".join(json.dumps(row) for row in rows))[0]
            return 25.0, digest, rows
        with mock.patch.object(bench_compare, "execute", side_effect=fake_execute):
            report = bench_compare.benchmark("fake", ["rustspark"], 2, 2, 1, 10)
        self.assertEqual(report["results"]["rustspark"]["scan_count"]["samples_ms"],
                         [25.0, 25.0])
        self.assertEqual(report["rustspark_median_over_competitor_median"], {})

    def test_invalid_sample_config(self):
        for config in [(0, 1, 0, 1), (1, 0, 0, 1), (1, 1, -1, 1), (1, 1, 0, 0)]:
            with self.assertRaises(ValueError):
                bench_compare.benchmark("fake", ["rustspark"], *config)


if __name__ == "__main__":
    unittest.main()
