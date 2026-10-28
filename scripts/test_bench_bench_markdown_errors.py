"""Escape untrusted benchmark failures in Markdown output."""
import unittest
import bench_query_latency as latency

class LatencyMarkdownTests(unittest.TestCase):
    def test_escapes_html_entities_and_table_delimiters(self):
        report = {"rows": 1, "repeat": 1, "warmup": 0, "metric": "cold",
                  "platform": "test", "revision": None, "warning": "test",
                  "results": [{"name": "scan_count", "status": "error",
                               "error": "<script>&copy;</script> | failed"}]}
        text = latency.render_markdown(report)
        self.assertIn("&lt;script&gt;&amp;copy;&lt;/script&gt;", text)
        self.assertIn(r"\| failed", text)
        self.assertNotIn("<script>", text)


if __name__ == "__main__":
    unittest.main()
