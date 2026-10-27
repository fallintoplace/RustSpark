import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import bench_query_latency as bench


def row_signature(rows):
    return sorted(json.dumps(row, sort_keys=True) for row in rows)


def reference_results(rows):
    data = [
        (index, *bench.benchmark_row(index)) for index in range(rows)
    ]
    counts = {}
    totals = {}
    for _, city, amount in data:
        counts[city] = counts.get(city, 0) + 1
        totals[city] = totals.get(city, 0) + amount
    filtered = [amount for _, _, amount in data if amount >= 70]
    ranks = []
    for city in counts:
        candidates = [(index, amount) for index, name, amount in data
                      if name == city and index < 256]
        for rank, (index, _) in enumerate(
            sorted(candidates, key=lambda item: (-item[1], item[0]))[:2], 1
        ):
            ranks.append({"city": city, "id": index, "rn": rank})
    top = sorted(data, key=lambda row: (-row[2], row[0]))[:10]
    filtered_sum = sum(filtered) if filtered else None
    return {
        "scan_count": [{"n": rows}],
        "filtered_sum": [{"total": filtered_sum}],
        "grouped_sum": [{"city": city, "total": total} for city, total in totals.items()],
        "distinct_cities": [{"n": len(counts)}],
        "range_filter": [{"n": sum(20 <= amount <= 79 for _, _, amount in data)}],
        "in_filter": [{"n": sum(city in {"city_1", "city_3", "city_7"}
                                for _, city, _ in data)}],
        "group_having": [{"city": city, "n": count} for city, count in counts.items()
                         if count >= 100],
        "top_k": [{"id": index, "amount": amount} for index, _, amount in top],
        "window_rank": ranks,
        "union_all": [{"n": rows}],
        "self_join": [{"total": filtered_sum}],
        "aggregate_join": [{"pairs": sum(count * count for count in counts.values())}],
    }


class QueryLatencyTests(unittest.TestCase):
    def test_all_queries_have_independent_answers_at_cycle_boundaries(self):
        self.assertEqual(len(bench.QUERIES), 12)
        for rows in (1, 15, 16, 99, 100, 255, 256, 399, 400, 401, 999,
                     1000, 1001, 1600, 3201):
            with self.subTest(rows=rows):
                actual = bench.expected_answers(rows)
                expected = reference_results(rows)
                self.assertEqual(set(actual), set(bench.QUERIES))
                for name in bench.QUERIES:
                    self.assertEqual(row_signature(actual[name]), row_signature(expected[name]), name)
                self.assertEqual(actual["top_k"], expected["top_k"])

    def test_large_row_count_uses_bounded_expected_work(self):
        expected = bench.expected_answers(10**9)
        self.assertEqual(expected["scan_count"], [{"n": 10**9}])
        self.assertEqual(len(expected["top_k"]), 10)
        self.assertEqual(len(expected["window_rank"]), 32)

    def test_measure_case_retains_only_measured_samples(self):
        expected = [{"n": 1}]
        digest = bench.output_signature('{"n":1}\n')[0]
        values = [(1.0, digest, expected), (2.0, digest, expected),
                  (3.0, digest, expected)]
        with mock.patch.object(bench, "execute", side_effect=values) as execute:
            result = bench.measure_case("binary", pathlib.Path("data.csv"),
                                        "scan_count", expected, 2, 1, 20)
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(result["samples_ms"], [2.0, 3.0])
        self.assertEqual(result["median_ms"], 2.5)
        self.assertEqual(result["p95_ms"], 3.0)
        self.assertEqual(result["result_rows"], 1)

    def test_incorrect_result_rejected_even_during_warmup(self):
        with mock.patch.object(bench, "execute", return_value=(1.0, "wrong", [])):
            with self.assertRaisesRegex(RuntimeError, "Incorrect SQL result"):
                bench.measure_case("binary", pathlib.Path("data.csv"),
                                   "scan_count", [{"n": 1}], 1, 1, 20)

    def test_ordered_topk_rejects_reordered_rows(self):
        expected = [{"id": 0, "amount": 0}, {"id": 1, "amount": 37}]
        digest = bench.output_signature("\n".join(json.dumps(row) for row in expected))[0]
        with mock.patch.object(bench, "execute",
                               return_value=(1.0, digest, list(reversed(expected)))):
            with self.assertRaisesRegex(RuntimeError, "Incorrect SQL ordering"):
                bench.measure_case("binary", pathlib.Path("data.csv"),
                                   "top_k", expected, 1, 0, 20)

    def test_invalid_duration_rejected(self):
        expected = [{"n": 1}]
        digest = bench.output_signature('{"n":1}')[0]
        for duration in (-1, float("nan"), float("inf"), True, "12"):
            with self.subTest(duration=duration):
                with mock.patch.object(bench, "execute",
                                       return_value=(duration, digest, expected)):
                    with self.assertRaisesRegex(ValueError, "Invalid duration"):
                        bench.measure_case("binary", pathlib.Path("data.csv"),
                                           "scan_count", expected, 1, 0, 20)

    def test_bad_options_fail_before_fixture_generation(self):
        with mock.patch.object(bench, "write_csv") as write_csv:
            for config in [(0, 1, 0, 20), (1, 0, 0, 20),
                           (1, 1, -1, 20), (1, 1, 0, 0)]:
                with self.assertRaises(ValueError):
                    bench.benchmark("binary", *config)
            for only in ([], ["unknown"], ["top_k", "top_k"]):
                with self.assertRaisesRegex(ValueError, "distinct, known"):
                    bench.benchmark("binary", 1, 1, 0, 20, only)
            write_csv.assert_not_called()

    def test_one_failed_query_does_not_hide_following_timings(self):
        measured = {"status": "ok", "samples_ms": [2.0], "median_ms": 2.0,
                    "p95_ms": 2.0, "result_sha256": "a" * 64, "result_rows": 1}
        with mock.patch.object(bench, "measure_case",
                               side_effect=[RuntimeError("bad query"), measured]):
            result = bench.benchmark("binary", 5, 1, 0, 20,
                                     ["scan_count", "filtered_sum"])
        self.assertEqual([case["status"] for case in result["results"]], ["error", "ok"])
        self.assertIn("bad query", result["results"][0]["error"])
        self.assertEqual(result["results"][1]["median_ms"], 2.0)
        self.assertEqual(len(result["csv_sha256"]), 64)
        rendered = bench.render_markdown(result)
        self.assertIn("ERROR: bad query", rendered)
        self.assertIn("2.000", rendered)

    def test_cli_writes_artifacts_and_fails_on_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            json_path = pathlib.Path(directory) / "results" / "result.json"
            md_path = pathlib.Path(directory) / "results" / "summary.md"
            with mock.patch.object(bench, "measure_case", side_effect=RuntimeError("oops")):
                code = bench.main([
                    "--rows", "1", "--repeat", "1", "--warmup", "0",
                    "--only", "scan_count", "--json-out", str(json_path),
                    "--markdown-out", str(md_path),
                ])
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(json_path.read_text())["results"][0]["status"], "error")
            self.assertIn("scan_count", md_path.read_text())


if __name__ == "__main__":
    unittest.main()
