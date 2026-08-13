import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from core.indicator_registry import nl_meta

class TestNlMeta(unittest.TestCase):
    def test_contains_example_queries(self):
        meta = nl_meta()
        self.assertIn("CLOSE > MA(CLOSE, 20)", meta["example_queries"])
        self.assertIn("PE_TTM < 20 AND TOTAL_MV > 1e10", meta["example_queries"])

    def test_units_include_market_value(self):
        meta = nl_meta()
        self.assertEqual(meta["units"]["TOTAL_MV"], "元")

    def test_route_importable(self):
        import py_compile
        routes_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api", "routes.py")
        py_compile.compile(routes_path, doraise=True)

if __name__ == "__main__":
    unittest.main()
