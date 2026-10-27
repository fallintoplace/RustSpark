import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import bench_report


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


if __name__ == "__main__":
    unittest.main()
