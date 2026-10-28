"""Markdown reporter must not destroy the input JSON report."""
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import bench_report

class MarkdownInputSafetyTests(unittest.TestCase):
    def test_same_path_fails_without_overwriting_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "result.json"
            before = '{"results":{}}\n'
            path.write_text(before)
            with mock.patch.object(bench_report, "render_report", return_value="# Report"):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    bench_report.main([str(path), "--output", str(path)])
            self.assertEqual(path.read_text(), before)

if __name__ == "__main__":
    unittest.main()
