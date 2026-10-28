"""Query errors and benchmark metadata must not inject Markdown content."""

import unittest

import bench_tpch_report as report


class MarkdownSafetyTests(unittest.TestCase):
    def test_error_messages_and_metric_are_escaped(self):
        payload = {
            "metric": "cold SQL\n# forged heading",
            "results": [
                {"id": 1, "status": "error",
                 "error": "<script>oops</script>\r\n# fake section | detail"},
            ],
        }
        output = report.render(payload)
        self.assertIn("**Metric:** cold SQL # forged heading", output)
        self.assertIn("&lt;script&gt;oops&lt;/script&gt;", output)
        self.assertNotIn("\n# forged heading", output)
        self.assertNotIn("\n# fake section", output)
        self.assertIn("fake section / detail", output)


if __name__ == "__main__":
    unittest.main()
