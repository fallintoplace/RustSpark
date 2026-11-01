"""Reject invalid ABBA timeouts before executing subprocesses."""

import unittest
from unittest import mock

import bench_tpch_abba as abba


class AbbaTimeoutTests(unittest.TestCase):
    def test_rejects_invalid_timeout_before_measuring(self):
        commands = {"baseline": ["old"], "candidate": ["new"]}
        with mock.patch.object(abba, "timed_command") as measure:
            for timeout in (0, -1, float("nan"), float("inf"), True):
                with self.subTest(timeout=timeout):
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        abba.compare_query(commands, False, 1, 0, timeout)
            measure.assert_not_called()


    def test_invalid_benchmark_options_reject_before_hashing_binaries(self):
        with mock.patch.object(abba, "binary_sha256") as sha:
            for settings in (
                {"repeat": 0}, {"repeat": True}, {"warmup": -1},
                {"warmup": 1.5}, {"timeout": float("nan")},
                {"timeout": float("inf")}, {"timeout": True},
                {"partitions": 0}, {"partitions": True},
                {"partitions": 1.5},
            ):
                with self.subTest(settings=settings), self.assertRaises(ValueError):
                    abba.benchmark("old", "new", "parquet", [], **settings)
            sha.assert_not_called()


if __name__ == "__main__":
    unittest.main()
