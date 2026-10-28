import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import bench_billion_rows as bench


class BillionRowBenchmarkTests(unittest.TestCase):
    def test_oracles_match_brute_force_at_boundaries(self):
        for rows in (1, 10, 42, 43, 364, 365, 366, 729_999, 730_000, 730_001):
            for day in (0, 42, 364):
                with self.subTest(rows=rows, day=day):
                    values = [i for i in range(rows) if i % bench.DAYS == day]
                    answers = bench.workloads(rows, day)
                    self.assertEqual(answers["partition_count"][1], [{"n": len(values)}])
                    self.assertEqual(answers["partition_sum"][1], [
                        {"total": sum(i % 10 for i in values) if values else None}])
                    self.assertEqual(answers["point_lookup"][1], [
                        {"n": sum(i % bench.CUSTOMERS == day for i in values)}])
                    self.assertEqual(answers["partition_top1"][1], [
                        {"largest": max(values) if values else None}])
                    self.assertEqual(answers["full_count"][1], [{"n": rows}])

    def test_billion_oracle_uses_constant_work(self):
        answers = bench.workloads(1_000_000_000, 42)
        self.assertEqual(answers["full_count"][1], [{"n": 1_000_000_000}])
        self.assertGreater(answers["partition_count"][1][0]["n"], 2_000_000)

    def test_rejects_bad_inputs(self):
        for rows, day in ((0, 1), (1, -1), (1, 365), (True, 1)):
            with self.assertRaises(ValueError):
                bench.workloads(rows, day)
        for only in ([], ["unknown"], ["full_count", "full_count"]):
            with self.assertRaises(ValueError):
                bench.measure("bin", pathlib.Path("no-need"), 10, 0, 1, 0, 10, None, only)

    def test_rejects_wrong_query_result_during_warmup(self):
        with mock.patch.object(bench, "run_query", return_value=(1.0, [{"n": 0}])) as query:
            report = bench.measure("bin", pathlib.Path("no-need"), 10,
                                   42, 3, 1, 10, None, ["full_count"])
        self.assertEqual(report["results"][0]["status"], "error")
        self.assertEqual(query.call_count, 1)

    def test_cold_samples_exclude_warmups(self):
        expected = [{"n": 10}]
        with mock.patch.object(bench, "run_query", side_effect=[
                (1.0, expected), (2.0, expected), (3.0, expected)]):
            report = bench.measure("bin", pathlib.Path("fixture"), 10,
                                   42, 2, 1, 10, 4, ["full_count"])
        result = report["results"][0]
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["samples_ms"], [2.0, 3.0])
        self.assertEqual(result["median_ms"], 2.5)
        self.assertEqual(result["p95_ms"], 3.0)

    def test_query_command_preserves_paths_and_options(self):
        command = bench.query_command("rustspark", pathlib.Path("/tmp/test"),
                                      "SELECT 1", 8)
        self.assertIn("--parquet-partitions", command)
        self.assertIn("day:int32", command)
        self.assertEqual(command[-2:], ["--target-partitions", "8"])


if __name__ == "__main__":
    unittest.main()
