import copy
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import bench_report
import sql_result_verify
import sql_report_compare


def measurement(median, digest="a" * 64):
    return {"median_ms": median, "p95_ms": median,
            "samples_ms": [median, median], "result_sha256": digest}


def report():
    return {
        "metric": "Cold process startup + SQL", "warning": "Not warm engine throughput",
        "rows": 100000, "repeat": 2, "warmup": 1, "platform": "test OS",
        "engines": ["rustspark", "duckdb", "spark"],
        "results": {
            "rustspark": {"scan_count": measurement(8), "group_by": measurement(4)},
            "duckdb": {"scan_count": measurement(4), "group_by": measurement(2)},
            "spark": {"scan_count": measurement(16), "group_by": measurement(12)},
        },
    }


class BenchmarkReportTests(unittest.TestCase):
    def test_cold_speed_ratio_direction_and_medians(self):
        rendered = bench_report.render_report(report())
        self.assertIn("| scan_count | 8.000 | 4.000 | 16.000 |", rendered)
        self.assertIn("| scan_count | 0.50x | 2.00x |", rendered)
        self.assertIn("below 1.00x means RustSpark was slower", rendered)
        self.assertIn("**Scope warning:** Not warm engine throughput", rendered)

    def test_warm_report_with_one_engine_is_explicit(self):
        sample = report()
        sample["metric"] = "Warm local SQL"
        sample["warning"] = "Excludes startup"
        sample.pop("engines")
        sample["results"] = {"rustspark": sample["results"]["rustspark"]}
        rendered = bench_report.render_report(sample)
        self.assertIn("Warm local SQL", rendered)
        self.assertIn("No competing engine was measured", rendered)
        self.assertNotIn("speed ratio", rendered)

    def test_escapes_untrusted_engine_headers_and_metadata(self):
        sample = report()
        sample["results"]["Duck|DB<test>"] = sample["results"].pop("duckdb")
        sample["metric"] = "Cold \\| <script>"
        rendered = bench_report.render_report(sample)
        self.assertIn(r"Duck\|DB&lt;test&gt;", rendered)
        self.assertIn("Cold " + "\\" * 3 + "| &lt;script&gt;", rendered)

    def test_rejects_invalid_warmup_metadata(self):
        for value in (-1, True, "1", 1.5):
            sample = report()
            sample["warmup"] = value
            with self.assertRaisesRegex(ValueError, "warmup"):
                bench_report.render_report(sample)

    def test_rejects_mismatched_workloads(self):
        sample = report()
        del sample["results"]["spark"]["group_by"]
        with self.assertRaisesRegex(ValueError, "Workloads differ"):
            bench_report.render_report(sample)

    def test_rejects_short_sample_lists(self):
        sample = report()
        sample["results"]["spark"]["scan_count"]["samples_ms"].pop()
        with self.assertRaisesRegex(ValueError, "Expected 2 samples"):
            bench_report.render_report(sample)

    def test_rejects_mismatched_result_hashes(self):
        sample = report()
        sample["results"]["spark"]["scan_count"]["result_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            bench_report.render_report(sample)

    def test_rejects_invalid_samples_and_medians(self):
        for invalid in (float("nan"), -1, "1.0", True):
            sample = report()
            sample["results"]["rustspark"]["scan_count"]["samples_ms"][0] = invalid
            with self.assertRaises(ValueError):
                bench_report.render_report(sample)
            sample = report()
            sample["results"]["rustspark"]["scan_count"]["median_ms"] = invalid
            with self.assertRaises(ValueError):
                bench_report.render_report(sample)

    def test_rejects_p95_smaller_than_median(self):
        sample = report()
        sample["results"]["rustspark"]["scan_count"]["p95_ms"] = 5
        with self.assertRaisesRegex(ValueError, "p95 is smaller"):
            bench_report.render_report(sample)

    def test_rejects_claimed_timing_not_in_samples(self):
        sample = report()
        sample["results"]["rustspark"]["scan_count"]["median_ms"] = 7.3
        with self.assertRaisesRegex(ValueError, "do not match samples"):
            bench_report.render_report(sample)

    def test_zero_rounded_rustspark_median_has_no_ratio(self):
        sample = report()
        sample["results"]["rustspark"]["scan_count"] = measurement(0)
        rendered = bench_report.render_report(sample)
        self.assertIn("| scan_count | n/a | n/a |", rendered)

    def test_rejects_missing_rustspark(self):
        sample = report()
        sample["results"].pop("rustspark")
        with self.assertRaisesRegex(ValueError, "RustSpark engine"):
            bench_report.render_report(sample)

    def test_rejects_duplicate_keys_and_nonfinite_constants_in_report_json(self):
        payload = json.dumps(report())
        self.assertEqual(bench_report.parse_report(payload), report())
        with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
            bench_report.parse_report('{"rows": 1, ' + payload[1:])
        with self.assertRaisesRegex(ValueError, "Non-finite"):
            bench_report.parse_report(payload.replace('"platform": "test OS"', '"platform": NaN'))

    def test_cli_writes_valid_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = pathlib.Path(tmp) / "bench.json"
            output = pathlib.Path(tmp) / "result.md"
            raw.write_text(json.dumps(report()), encoding="utf-8")
            self.assertEqual(bench_report.main([str(raw), "--output", str(output)]), 0)
            self.assertIn("# Local SQL benchmark comparison", output.read_text())



def sql_report_sample():
    return {
        "query_file_sha256": "d" * 64,
        "queries_attempted": 2,
        "results": [
            {"id": 1, "sql": "SELECT 1;", "status": "executed",
             "result_sha256": "a" * 64, "result_rows": 1},
            {"id": 2, "sql": "SELECT 2;", "status": "executed",
             "result_sha256": "b" * 64, "result_rows": 2},
        ],
    }


class SQLResultVerificationTests(unittest.TestCase):
    def test_accepts_same_results_in_different_query_order(self):
        reference = sql_report_sample()
        actual = sql_report_sample()
        actual["results"].reverse()
        self.assertEqual(sql_result_verify.verify_reports(actual, reference), [])

    def test_detects_result_digest_and_count_changes(self):
        reference = sql_report_sample()
        actual = sql_report_sample()
        actual["results"][0]["result_sha256"] = "c" * 64
        actual["results"][0]["result_rows"] = 3
        issues = sql_result_verify.verify_reports(actual, reference)
        self.assertTrue(any("hash differs" in issue for issue in issues))
        self.assertTrue(any("row count differs" in issue for issue in issues))

    def test_checks_workload_hash_and_sql_text(self):
        reference = sql_report_sample()
        actual = sql_report_sample()
        actual["query_file_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "workload hash"):
            sql_result_verify.verify_reports(actual, reference)
        actual = sql_report_sample()
        actual["results"][0]["sql"] = "SELECT 9;"
        self.assertIn("SQL differs", sql_result_verify.verify_reports(actual, reference)[0])

    def test_rejects_duplicate_ids_and_invalid_reference_rows(self):
        reference = sql_report_sample()
        actual = sql_report_sample()
        actual["results"][1]["id"] = 1
        with self.assertRaisesRegex(ValueError, "duplicate query ID"):
            sql_result_verify.verify_reports(actual, reference)
        actual = sql_report_sample()
        reference["results"][0]["status"] = "error"
        self.assertIn("invalid reference", sql_result_verify.verify_reports(actual, reference)[0])

    def test_strict_json_loader_and_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            actual_path = root / "actual.json"
            reference_path = root / "reference.json"
            payload = json.dumps(sql_report_sample())
            actual_path.write_text(payload, encoding="utf-8")
            reference_path.write_text(payload, encoding="utf-8")
            self.assertEqual(sql_result_verify.main([
                "--actual", str(actual_path), "--reference", str(reference_path),
            ]), 0)
            actual_path.write_text('{"results": [], "results": []}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Duplicate JSON key"):
                sql_result_verify.load_report(actual_path)



class SQLReportComparisonTests(unittest.TestCase):
    def reports(self):
        reference = sql_report_sample()
        candidate = copy.deepcopy(reference)
        for result in reference["results"]:
            result["median_ms"] = 1.0
        for result in candidate["results"]:
            result["median_ms"] = 1.0
        return reference, candidate

    def test_rejects_invalid_matching_result_digests_and_counts(self):
        for value in ("", "not-a-sha"):
            reference, candidate = self.reports()
            reference["results"][0]["result_sha256"] = value
            candidate["results"][0]["result_sha256"] = value
            result = sql_report_compare.compare_reports(reference, candidate)
            self.assertIn("result differs", result["errors"][0])
        for value in (-1, True, None):
            reference, candidate = self.reports()
            reference["results"][0]["result_rows"] = value
            candidate["results"][0]["result_rows"] = value
            result = sql_report_compare.compare_reports(reference, candidate)
            self.assertIn("result differs", result["errors"][0])

    def test_zero_baseline_with_positive_candidate_is_a_slowdown(self):
        reference, candidate = self.reports()
        reference["results"][0]["median_ms"] = 0.0
        candidate["results"][0]["median_ms"] = 2.0
        result = sql_report_compare.compare_reports(reference, candidate, 1.5)
        self.assertEqual(result["slowdown_query_ids"], [1])
        self.assertIsNone(result["timings"][0]["slowdown"])

    def test_zero_baseline_with_zero_candidate_is_not_a_slowdown(self):
        reference, candidate = self.reports()
        reference["results"][0]["median_ms"] = 0.0
        candidate["results"][0]["median_ms"] = 0.0
        result = sql_report_compare.compare_reports(reference, candidate, 1.5)
        self.assertEqual(result["slowdown_query_ids"], [])

    def test_rejects_malformed_workload_digest(self):
        reference, candidate = self.reports()
        reference["query_file_sha256"] = "same"
        candidate["query_file_sha256"] = "same"
        with self.assertRaisesRegex(ValueError, "SQL workloads"):
            sql_report_compare.compare_reports(reference, candidate)


if __name__ == "__main__":
    unittest.main()
