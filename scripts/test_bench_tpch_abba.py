"""Verify alternating A/B trials detect changed answers and preserve sample counts."""

import unittest
from unittest import mock

import bench_tpch_abba as abba


class AbbaTests(unittest.TestCase):
    def test_interleaves_engines_and_uses_measured_only(self):
        commands = {"baseline": ["old"], "candidate": ["new"]}
        seen = []
        def timed(cmd, timeout, ordered):
            seen.append(cmd[0])
            return (4.0 if cmd[0] == "old" else 2.0, "same", 1)
        with mock.patch.object(abba, "timed_command", side_effect=timed):
            report = abba.compare_query(commands, False, repeat=2, warmup=1, timeout=120)
        self.assertEqual(seen, ["old", "new", "new", "old"] * 3)
        self.assertEqual(report["baseline_samples_ms"], [4, 4, 4, 4])
        self.assertEqual(report["candidate_samples_ms"], [2, 2, 2, 2])
        self.assertEqual(report["baseline_over_candidate"], 2.0)

    def test_result_mismatch_blocks_score(self):
        commands = {"baseline": ["old"], "candidate": ["new"]}
        with mock.patch.object(abba, "timed_command", side_effect=[
            (1.0, "a", 1), (0.9, "b", 1),
        ]):
            with self.assertRaisesRegex(ValueError, "different SQL results"):
                abba.compare_query(commands, False, 1, 0, 120)

    def test_invalid_repeats_and_zero_time(self):
        with self.assertRaises(ValueError):
            abba.compare_query({}, False, 0, 0, 120)
        fake = mock.Mock(returncode=0, stdout='{"n":1}\n', stderr="")
        with mock.patch.object(abba.subprocess, "run", return_value=fake):
            with mock.patch.object(abba.time, "perf_counter", side_effect=[1, 1]):
                with self.assertRaisesRegex(ValueError, "Invalid"):
                    abba.timed_command(["rs"], 30, False)

    def test_nonzero_process_exits_fail(self):
        fake = mock.Mock(returncode=4, stdout="", stderr="bad query")
        with mock.patch.object(abba.subprocess, "run", return_value=fake):
            with self.assertRaisesRegex(RuntimeError, "bad query"):
                abba.timed_command(["rs"], 30, False)


if __name__ == "__main__":
    unittest.main()
