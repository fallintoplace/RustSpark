import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import ballista_smoke


class BallistaSmokeTests(unittest.TestCase):
    def test_command_selection(self):
        standalone = ballista_smoke.command_for("standalone", None)
        self.assertIn("--example", standalone)
        self.assertEqual(standalone[-1], "standalone")
        self.assertEqual(
            ballista_smoke.command_for("remote", "/tmp/ballista-client"),
            ["/tmp/ballista-client"],
        )
        with self.assertRaisesRegex(ValueError, "requires --client"):
            ballista_smoke.command_for("remote", None)

    def test_output_check_is_strict(self):
        good = "+---------+----------------+\n| matches | weighted_total |\n| 2 | 80 |\n"
        ballista_smoke.verify_output(good)
        for bad in (
            "| matches | weighted_total |\n| 1 | 80 |",
            "| matches | weighted_total |\n| 2 | 81 |",
            "| 2 | 80 |",
            "| matches | weighted_total |\n",
        ):
            with self.assertRaisesRegex(RuntimeError, "expected|did not return"):
                ballista_smoke.verify_output(bad)

    def test_rejects_disconnected_headers_and_extra_data_rows(self):
        for output in (
            "matches weighted_total\n| 2 | 80 |\n",
            "| matches | weighted_total |\n| 2 | 80 |\n| 9 | 99 |\n",
            "| weighted_total | matches |\n| 80 | 2 |\n",
        ):
            with self.assertRaisesRegex(RuntimeError, "expected"):
                ballista_smoke.verify_output(output)

    def test_fixture_values_match_expected_aggregate(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            orders, rates = ballista_smoke.write_fixtures(pathlib.Path(tmp))
            self.assertEqual(len(orders.read_text().splitlines()), 5)
            self.assertIn("2,3", rates.read_text())

    @mock.patch.object(ballista_smoke.subprocess, "run")
    def test_runs_with_named_shared_sources(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout="| matches | weighted_total |\n| 2 | 80 |\n",
            stderr="",
        )
        ballista_smoke.run_smoke("standalone", timeout=15)
        args, kwargs = run.call_args
        self.assertEqual(kwargs["timeout"], 15)
        self.assertEqual(kwargs["cwd"], ballista_smoke.ROOT)
        self.assertEqual(kwargs["env"]["RUSTSPARK_SQL"], ballista_smoke.QUERY)
        self.assertIn("csv:orders=", kwargs["env"]["RUSTSPARK_INPUTS"])
        self.assertIn(";csv:rates=", kwargs["env"]["RUSTSPARK_INPUTS"])
        self.assertEqual(kwargs["env"]["RUSTSPARK_STANDALONE_PARALLELISM"], "4")
        self.assertFalse(pathlib.Path(kwargs["env"]["RUSTSPARK_INPUTS"].split("=")[1].split(";")[0]).exists())

    @mock.patch.object(ballista_smoke.subprocess, "run")
    def test_remote_client_keeps_callers_working_directory(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout="| matches | weighted_total |\n| 2 | 80 |\n",
            stderr="",
        )
        ballista_smoke.run_smoke("remote", "./bin/ballista-client", timeout=20)
        self.assertEqual(run.call_args.args[0], ["./bin/ballista-client"])
        self.assertIsNone(run.call_args.kwargs["cwd"])

    @mock.patch.object(ballista_smoke.subprocess, "run")
    def test_failure_is_not_accepted(self, run):
        run.return_value = mock.Mock(returncode=12, stdout="", stderr="failed")
        with self.assertRaisesRegex(RuntimeError, "code 12"):
            ballista_smoke.run_smoke("standalone")
        with self.assertRaisesRegex(ValueError, "timeout"):
            ballista_smoke.run_smoke("standalone", timeout=0)


    @mock.patch.object(ballista_smoke.subprocess, "run")
    def test_ignores_inherited_query_settings(self, run):
        run.return_value = mock.Mock(
            returncode=0,
            stdout="| matches | weighted_total |\n| 2 | 80 |\n",
            stderr="",
        )
        inherited = {
            "RUSTSPARK_PARQUET": "/tmp/unrelated.parquet",
            "RUSTSPARK_MAX_ROWS": "1",
            "RUSTSPARK_QUERY_TIMEOUT_SECS": "1",
            "RUSTSPARK_INPUTS_FILE": "/tmp/unrelated-inputs.txt",
            "RUSTSPARK_SCHEDULER_URLS": "df://wrong:55555",
            "RUSTSPARK_SCHEDULER_CONNECT_TIMEOUT_SECS": "1",
            "RUSTSPARK_OUTPUT_ARROW": "/tmp/unrelated.arrow",
        }
        with mock.patch.dict(ballista_smoke.os.environ, inherited):
            ballista_smoke.run_smoke("standalone")
        env = run.call_args.kwargs["env"]
        for name in inherited:
            self.assertNotIn(name, env)


if __name__ == "__main__":
    unittest.main()
