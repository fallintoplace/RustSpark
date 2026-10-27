"""Warm TPC-H parses per-query result boundaries and shell statement timers."""

import json
from pathlib import Path
import tempfile
import unittest

import bench_tpch_warm as warm


class WarmTpchTests(unittest.TestCase):
    def requests(self):
        with tempfile.TemporaryDirectory() as directory:
            sql = Path(directory) / "01.sql"
            sql.write_text("SELECT 42 AS value;", encoding="utf-8")
            return warm.requests_for([(1, sql)], 1, 2)

    def output(self, requests, values=None):
        values = values or [42] * len(requests)
        rows = []
        for index, value in enumerate(values):
            rows.extend([
                {warm.BOUNDARY: f"start:{index}"},
                {"value": value},
                {warm.BOUNDARY: f"end:{index}"},
            ])
        stdout = "\n".join(json.dumps(row) for row in rows)
        stderr = "Time: 0.10 ms\nTime: 1.25 ms\nTime: 0.10 ms\n" * len(requests)
        return stdout, stderr

    def test_parses_warm_samples_and_checks_results(self):
        requests = self.requests()
        self.assertEqual(len(requests), 3)
        self.assertIn(".timer on", warm.shell_script(requests))
        stdout, stderr = self.output(requests)
        results = warm.parse_shell(stdout, stderr, requests, 1, 2)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "ok")
        self.assertEqual(results[0]["samples_ms"], [1.25, 1.25])
        self.assertEqual(results[0]["result_rows"], 1)

    def test_rejects_query_errors_missing_timers_and_boundaries(self):
        requests = self.requests()
        stdout, stderr = self.output(requests)
        for s, e in ((stdout, "SQL error: query failed"), (stdout, stderr[:-14]),
                     (stdout.replace("start:0", "start:99"), stderr)):
            with self.subTest(stderr=e[:20]), self.assertRaises((ValueError, RuntimeError)):
                warm.parse_shell(s, e, requests, 1, 2)

    def test_detects_changed_warm_results(self):
        requests = self.requests()
        stdout, stderr = self.output(requests, [42, 42, 43])
        with self.assertRaisesRegex(ValueError, "Nondeterministic"):
            warm.parse_shell(stdout, stderr, requests, 1, 2)

    def test_input_validation(self):
        with self.assertRaises(ValueError):
            warm.requests_for([], 1, 3)
        with self.assertRaises(ValueError):
            warm.requests_for([], -1, 3)


if __name__ == "__main__":
    unittest.main()
