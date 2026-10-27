"""Test full-suite driver commands and incomplete-run detection."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_tpch_smoke as smoke


class SmokeTests(unittest.TestCase):
    def test_command_selects_entire_suite(self):
        prep, runner = smoke.commands("rs", Path("/tmp/fixture"), Path("/tmp/out.json"), 0.01, 4, 30)
        self.assertIn("tpch_prepare.py", prep[1])
        self.assertIn("tpch_run.py", runner[1])
        self.assertIn("--json-out", runner)
        self.assertNotIn("--query-ids", runner)
        self.assertEqual(runner[runner.index("--repeat") + 1], "1")

    def test_smoke_rejects_missing_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "fixture"
            report = Path(tmp) / "report.json"
            def run_side_effect(command, **kwargs):
                if "--json-out" in command:
                    report.write_text(json.dumps({"results": [
                        {"id": n, "status": "ok"} for n in range(1, 22)
                    ]}))
                return mock.Mock(returncode=0)
            with mock.patch.object(smoke.subprocess, "run", side_effect=run_side_effect):
                self.assertFalse(smoke.run_smoke("rs", fixture, report))

    def test_smoke_accepts_all_successes(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "fixture"
            report = Path(tmp) / "report.json"
            def run_side_effect(command, **kwargs):
                if "--json-out" in command:
                    report.write_text(json.dumps({"results": [
                        {"id": n, "status": "ok"} for n in range(1, 23)
                    ]}))
                return mock.Mock(returncode=0)
            with mock.patch.object(smoke.subprocess, "run", side_effect=run_side_effect):
                self.assertTrue(smoke.run_smoke("rs", fixture, report))

    def test_rejects_existing_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "must not already exist"):
                smoke.run_smoke("rs", Path(tmp), Path(tmp) / "out.json")


if __name__ == "__main__":
    unittest.main()
