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


if __name__ == "__main__":
    unittest.main()
