import json
import pathlib
import tempfile
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import bench_compare
import sql_report_compare


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

    def test_unknown_engine_fails_before_writing_fixtures(self):
        with mock.patch.object(bench_compare, "write_csv") as write_csv:
            with self.assertRaisesRegex(ValueError, "Select unique engines"):
                bench_compare.benchmark("fake", ["unknown"], 100000, 1, 0, 10)
            write_csv.assert_not_called()

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
        self.assertEqual(report["competitor_median_over_rustspark_median"], {})

    def test_measurement_checks_warmups_and_retains_samples(self):
        values = [(1.0, "ok", []), (2.0, "ok", []), (3.0, "ok", [])]
        with mock.patch.object(bench_compare, "execute", side_effect=values) as execute:
            result = bench_compare.measure_engine_workload(
                "rustspark", "scan_count", ["binary"], "ok", 2, 1, 10
            )
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(result["samples_ms"], [2.0, 3.0])
        self.assertEqual(result["median_ms"], 2.5)

    def test_rejects_invalid_benchmark_durations(self):
        for duration in (-1.0, float("nan"), float("inf"), True):
            with mock.patch.object(bench_compare, "execute",
                                   return_value=(duration, "ok", [])):
                with self.assertRaisesRegex(ValueError, "Invalid benchmark duration"):
                    bench_compare.measure_engine_workload(
                        "rustspark", "scan_count", ["binary"], "ok", 1, 0, 10
                    )

    def test_invalid_sample_config(self):
        for config in [(0, 1, 0, 1), (1, 0, 0, 1), (1, 1, -1, 1), (1, 1, 0, 0)]:
            with self.assertRaises(ValueError):
                bench_compare.benchmark("fake", ["rustspark"], *config)


    def test_raw_ratio_matches_report_direction_and_handles_zero(self):
        def measurement(engine, workload, command, expected, repeat, warmup, timeout):
            median = 10.0 if engine == "rustspark" else 20.0
            if workload == "scan_count":
                median = 0.0 if engine == "rustspark" else 20.0
            return {"median_ms": median, "p95_ms": median,
                    "samples_ms": [median], "result_sha256": expected}

        with mock.patch.object(bench_compare, "check_dependencies"), mock.patch.object(
            bench_compare, "measure_engine_workload", side_effect=measurement
        ):
            result = bench_compare.benchmark(
                "fake", ["rustspark", "duckdb"], 2, 1, 0, 10
            )

        ratios = result["competitor_median_over_rustspark_median"]["duckdb"]
        self.assertIsNone(ratios["scan_count"])
        for name in bench_compare.WORKLOADS:
            if name != "scan_count":
                self.assertEqual(ratios[name], 2.0)
        self.assertNotIn("rustspark_median_over_competitor_median", result)


def sample_query_report(medians=(10.0, 4.0)):
    return {
        "query_file_sha256": "f" * 64,
        "queries_attempted": 2,
        "results": [
            {"id": 1, "sql": "SELECT 1;", "status": "executed",
             "median_ms": medians[0], "result_sha256": "a" * 64, "result_rows": 1},
            {"id": 2, "sql": "SELECT 2;", "status": "executed",
             "median_ms": medians[1], "result_sha256": "b" * 64, "result_rows": 2},
        ],
    }


class SQLLatencyComparisonTests(unittest.TestCase):
    def test_matching_results_produce_latency_ratios(self):
        reference = sample_query_report()
        candidate = sample_query_report((20.0, 2.0))
        candidate["results"].reverse()
        result = sql_report_compare.compare_reports(reference, candidate)
        self.assertEqual(result["queries_compared"], 2)
        self.assertEqual(result["errors"], [])
        self.assertEqual([row["slowdown"] for row in result["timings"]], [2.0, 0.5])

    def test_reports_result_mismatch_before_speed_ratio(self):
        reference = sample_query_report()
        candidate = sample_query_report()
        candidate["results"][0]["result_sha256"] = "c" * 64
        result = sql_report_compare.compare_reports(reference, candidate)
        self.assertEqual(result["queries_compared"], 1)
        self.assertIn("result differs", result["errors"][0])

    def test_threshold_and_zero_baseline(self):
        reference = sample_query_report((0.0, 10.0))
        candidate = sample_query_report((0.0, 13.0))
        result = sql_report_compare.compare_reports(reference, candidate, 1.25)
        self.assertEqual(result["slowdown_query_ids"], [2])
        self.assertIsNone(result["timings"][0]["slowdown"])
        with self.assertRaisesRegex(ValueError, "at least 1"):
            sql_report_compare.compare_reports(reference, candidate, 0.5)

    def test_rejects_missing_queries_and_invalid_latencies(self):
        reference = sample_query_report()
        candidate = sample_query_report()
        candidate["results"][0]["median_ms"] = float("nan")
        self.assertIn("invalid latency", sql_report_compare.compare_reports(
            reference, candidate,
        )["errors"][0])
        candidate = sample_query_report()
        candidate["results"].pop()
        candidate["queries_attempted"] = 1
        self.assertIn("missing in one report", sql_report_compare.compare_reports(
            reference, candidate,
        )["errors"][0])

    def test_cli_output_and_strict_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            reference = root / "reference.json"
            candidate = root / "candidate.json"
            output = root / "comparison.json"
            reference.write_text(json.dumps(sample_query_report()), encoding="utf-8")
            candidate.write_text(json.dumps(sample_query_report((12.0, 4.0))), encoding="utf-8")
            self.assertEqual(sql_report_compare.main([
                str(reference), str(candidate), "--max-slowdown", "1.1",
                "--json-out", str(output),
            ]), 1)
            self.assertEqual(json.loads(output.read_text())["slowdown_query_ids"], [1])
            candidate.write_text('{"results": [], "results": []}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
                sql_report_compare.load_report(candidate)


if __name__ == "__main__":
    unittest.main()
