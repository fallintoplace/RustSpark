"""Missing local Ballista executable should return a clean CLI error."""
import contextlib
import io
import unittest
from unittest import mock
import ballista_smoke as smoke


class BallistaMissingExecutableTests(unittest.TestCase):
    def test_missing_cargo_fails_without_traceback(self):
        stderr = io.StringIO()
        with mock.patch.object(smoke.subprocess, "run", side_effect=FileNotFoundError("cargo not installed")):
            with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                smoke.main([])
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("FAIL:", stderr.getvalue())
        self.assertIn("cargo not installed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
