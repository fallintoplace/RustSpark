"""Matching but malformed hashes cannot certify benchmark equivalence."""

import unittest

import bench_tpch_regress as regress


class RegressionFingerprintShapeTests(unittest.TestCase):
    def test_matching_invalid_hashes_are_rejected(self):
        for key in ("sql_sha256", "result_sha256"):
            for digest in ("not-a-sha256", "A" * 64, 12):
                with self.subTest(key=key, digest=digest):
                    old = {"results": [{"id": 1, "status": "ok", "median_ms": 10, key: digest}]}
                    new = {"results": [{"id": 1, "status": "ok", "median_ms": 5, key: digest}]}
                    with self.assertRaisesRegex(ValueError, "invalid .*fingerprint"):
                        regress.compare(old, new)


if __name__ == "__main__":
    unittest.main()
