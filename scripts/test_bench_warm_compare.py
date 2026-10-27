import json
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import bench_warm_compare as warm
from bench_rustspark import WORKLOADS, expected_results


def fixture(rows, warmup=0, repeat=1):
    requests = warm.requests_for(warmup, repeat)
    values = expected_results(rows)
    samples = [
        {"name": request["name"], "elapsed_ms": i + 1.0,
         "rows": values[request["name"]]}
        for i, request in enumerate(requests)
    ]
    return requests, samples


class WarmComparisonTests(unittest.TestCase):
    def test_repeats_each_query_in_one_session(self):
        requests = warm.requests_for(1, 2)
        self.assertEqual(len(requests), 3 * len(WORKLOADS))
        self.assertEqual([item["name"] for item in requests[:3]], ["scan_count"] * 3)

    def test_parser_handles_multirow_group_by_output(self):
        requests, samples = fixture(35)
        stdout = "\n".join(json.dumps(row) for sample in samples
                           for row in sample["rows"])
        stderr = "\n".join(f"Time: {item['elapsed_ms']:.3f} ms" for item in samples)
        self.assertEqual(warm.parse_rustspark(stdout, stderr, requests, 35), samples)
        self.assertGreater(len(samples[2]["rows"]), 1)

    def test_rejects_duplicate_and_nonfinite_warm_results(self):
        requests, samples = fixture(3)
        stdout = "\n".join(
            json.dumps(row) for sample in samples for row in sample["rows"]
        )
        stderr = "\n".join("Time: 1.000 ms" for _ in requests)
        for bad in ('{"n": 3, "n": 3}', '{"n": NaN}'):
            invalid = stdout.replace('{"n": 3}', bad, 1)
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                warm.parse_rustspark(invalid, stderr, requests, 3)
        with self.assertRaises(ValueError):
            warm.parse_result_line('{"name": "x", "name": "x"}')

    def test_rejects_missing_timers_and_sql_failures(self):
        requests, _ = fixture(2)
        with self.assertRaisesRegex(ValueError, "timers"):
            warm.parse_rustspark("", "Time: 1.0 ms", requests, 2)
        with self.assertRaisesRegex(RuntimeError, "SQL failed"):
            warm.parse_rustspark("", "SQL error: missing", requests, 2)

    def test_detects_bad_results_and_nonfinite_timings(self):
        requests, samples = fixture(3)
        samples[0]["rows"] = [{"n": 99}]
        with self.assertRaisesRegex(RuntimeError, "Incorrect result"):
            warm.summarize(samples, requests, 3, 0, 1)
        _, samples = fixture(3)
        samples[0]["elapsed_ms"] = float("nan")
        with self.assertRaisesRegex(ValueError, "Invalid duration"):
            warm.summarize(samples, requests, 3, 0, 1)

    def test_warmup_is_not_counted(self):
        requests, samples = fixture(3, warmup=1, repeat=2)
        report = warm.summarize(samples, requests, 3, 1, 2)
        self.assertEqual(report["scan_count"]["samples_ms"], [2.0, 3.0])
        self.assertEqual(report["scan_count"]["median_ms"], 2.5)

    @mock.patch.object(warm.subprocess, "run")
    def test_single_rustspark_interactive_process(self, run):
        requests, samples = fixture(3)
        run.return_value = mock.Mock(
            returncode=0,
            stdout="\n".join(json.dumps(row) for sample in samples
                             for row in sample["rows"]),
            stderr="\n".join("Time: 1.000 ms" for _ in samples),
        )
        warm.run_rustspark("rustspark", pathlib.Path("/tmp/data.csv"),
                           requests, 3, 20, 2)
        command = run.call_args.args[0]
        self.assertIn("--interactive", command)
        self.assertIn("--stdout-format", command)
        self.assertIn(".timer on", run.call_args.kwargs["input"])
        self.assertIn(".quit", run.call_args.kwargs["input"])

    @mock.patch.object(warm, "run_rustspark")
    def test_benchmark_rejects_incorrect_engine_output(self, run):
        requests, samples = fixture(2)
        samples[0]["rows"] = [{"n": -1}]
        run.return_value = samples
        with self.assertRaisesRegex(RuntimeError, "Incorrect result"):
            warm.benchmark("bin", ["rustspark"], 2, 0, 1, 10, 2, "local[2]")


    @mock.patch.object(warm.importlib.util, "find_spec", return_value=None)
    def test_duckdb_dependency_error(self, _find_spec):
        with self.assertRaisesRegex(RuntimeError, "DuckDB"):
            warm.run_duckdb(pathlib.Path("/tmp/input.csv"), [], 10)

    def test_duckdb_uses_one_session_and_checks_results(self):
        requests, samples = fixture(16)
        result = mock.Mock(
            returncode=0, stdout="\n".join(json.dumps(s) for s in samples), stderr=""
        )
        with mock.patch.object(warm.importlib.util, "find_spec", return_value=object()), \
             mock.patch.object(warm.subprocess, "run", return_value=result) as command:
            values = warm.run_duckdb(pathlib.Path("/tmp/input.csv"), requests, 10)
        self.assertEqual(values, samples)
        self.assertEqual(command.call_count, 1)
        self.assertIn("read_csv", command.call_args.args[0][2])
        self.assertEqual(warm.summarize(values, requests, 16, 0, 1)["scan_count"]["median_ms"], 1.0)

    def test_duckdb_ratio_direction(self):
        measurements = []
        def fake_summarize(samples, requests, rows, warmup, repeat):
            value = 10.0 if not measurements else 15.0
            measurements.append(value)
            return {name: {"median_ms": value} for name in WORKLOADS}
        with mock.patch.object(warm, "run_rustspark", return_value=[]), \
             mock.patch.object(warm, "run_duckdb", return_value=[]), \
             mock.patch.object(warm, "summarize", side_effect=fake_summarize):
            report = warm.benchmark("bin", ["rustspark", "duckdb"], 2, 0, 1, 10, 2, "local[2]")
        self.assertEqual(set(report["duckdb_median_over_rustspark_median"].values()), {1.5})

    def test_validation_rejects_bad_runs(self):
        for cfg in [(0, 0, 1, 10, 2), (2, -1, 1, 10, 2),
                    (2, 0, 0, 10, 2), (2, 0, 1, 10, 0)]:
            with self.assertRaises(ValueError):
                warm.benchmark("bin", ["rustspark"], *cfg, "local[2]")


    def test_raw_ratio_matches_markdown_direction_and_zero_is_null(self):
        def summarizer(samples, requests, rows, warmup, repeat):
            median = 10.0 if len(calls) == 0 else 20.0
            calls.append(median)
            return {
                name: {"median_ms": (0.0 if median == 10.0 and name == "scan_count"
                                    else median)}
                for name in WORKLOADS
            }

        calls = []
        with mock.patch.object(warm, "run_rustspark", return_value=[]), mock.patch.object(
            warm, "run_spark", return_value=[]
        ), mock.patch.object(warm, "summarize", side_effect=summarizer):
            report = warm.benchmark(
                "fake", ["rustspark", "spark"], 2, 0, 1, 10, 2, "local[2]"
            )
        ratios = report["spark_median_over_rustspark_median"]
        self.assertIsNone(ratios["scan_count"])
        for name in WORKLOADS:
            if name != "scan_count":
                self.assertEqual(ratios[name], 2.0)
        self.assertNotIn("rustspark_median_over_spark_median", report)

if __name__ == "__main__":
    unittest.main()
