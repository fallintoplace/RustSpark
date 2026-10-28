"""Cycle-based star benchmark oracle agrees with brute-force joins."""
import unittest
from bench_star_schema import answers

class FastStarOracleTests(unittest.TestCase):
    def test_matches_brute_force_answers(self):
        for rows, products, regions in [(1, 1, 1), (99, 7, 3), (101, 10, 7),
                                        (777, 17, 23), (10001, 9, 2)]:
            with self.subTest(rows=rows, products=products, regions=regions):
                out = answers(rows, products, regions)
                grouped = {}
                for index in range(rows):
                    region = (index % products) % regions
                    grouped[region] = grouped.get(region, 0) + (index * 17) % 100
                self.assertEqual(out["region_revenue"], [
                    {"region": key, "total": value} for key, value in sorted(grouped.items())
                ])
                self.assertEqual(out["three_table_sum"][0]["total"], sum(grouped.values()))

    def test_large_size_does_not_need_billion_row_loop(self):
        out = answers(10**9, 10, 2)
        self.assertEqual(out["fact_product_count"], [{"n": 10**9}])
        self.assertEqual(sum(x["total"] for x in out["region_revenue"]),
                         out["three_table_sum"][0]["total"])

if __name__ == "__main__":
    unittest.main()
