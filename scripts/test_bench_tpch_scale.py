"""Validate multi-scale arguments and command construction."""

from pathlib import Path
import unittest

import bench_tpch_scale as sweep


class ScaleTests(unittest.TestCase):
    def test_scales(self):
        self.assertEqual(sweep.scales("0.01,0.1,1"), [0.01, 0.1, 1.0])
        for item in ("", "0", "-1", "nan", "inf", "1,1", "one"):
            with self.subTest(value=item), self.assertRaises(ValueError):
                sweep.scales(item)

    def test_commands(self):
        fixture, report, prepare, run = sweep.commands(0.1, "rs", Path("/tmp/out"))
        self.assertEqual(fixture.name, "sf-0.1")
        self.assertEqual(report.name, "sf-0.1.json")
        self.assertIn("tpch_prepare.py", prepare[1])
        self.assertIn("tpch_run.py", run[1])
        self.assertNotIn("--query-ids", run)

    def test_full_suite_requires_measurement_evidence(self):
        from tempfile import TemporaryDirectory
        from unittest import mock
        import json
        with TemporaryDirectory() as tmp:
            report = Path(tmp) / "run.json"
            rows = [{"id": n, "status": "ok"} for n in range(1, 23)]

            def run(command, **kwargs):
                report.write_text(json.dumps({"results": rows}), encoding="utf-8")
                return mock.Mock(returncode=0)

            with mock.patch.object(sweep.subprocess, "run", side_effect=run):
                self.assertEqual(sweep.run_scale(["prepare"], ["run"], report)["status"], "error")
                for row in rows:
                    row.update({"median_ms": 1.0, "result_rows": 1,
                                "sql_sha256": "a" * 64, "result_sha256": "b" * 64})
                self.assertEqual(sweep.run_scale(["prepare"], ["run"], report)["status"], "ok")

    def test_rejects_duplicate_successful_query_ids(self):
        import json
        from tempfile import TemporaryDirectory
        from unittest import mock
        with TemporaryDirectory() as tmp:
            report = Path(tmp) / "result.json"
            rows = [{"id": n, "status": "ok"} for n in range(1, 22)]
            rows.append({"id": 1, "status": "ok"})
            def run(command, **kwargs):
                report.write_text(json.dumps({"results": rows}))
                return mock.Mock(returncode=0)
            with mock.patch.object(sweep.subprocess, "run", side_effect=run):
                result = sweep.run_scale(["prepare"], ["run"], report)
            self.assertEqual(result["passed"], 22)
            self.assertEqual(result["status"], "error")


if __name__ == "__main__":
    unittest.main()
