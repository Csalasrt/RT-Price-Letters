"""Checks the LB/GAL cost and price logic behind the Printer page.

Run from the project root:  venv/bin/python -m unittest discover tests

Uses a throwaway SQLite database, and the LB/GAL weights below are test
inputs only. No real product data is read or changed.
"""
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

_tmp = tempfile.TemporaryDirectory()
os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(_tmp.name, "test.db")
os.environ.pop("RENDER", None)
os.environ.pop("FLASK_ENV", None)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module  # noqa: E402

WEIGHTS = {"ddbsa": 8.9, "2eh": 6.9}
LB_PRICED = {"ddbsa": [{"product": "DDBSA", "um": "LB", "cost": 1.00}]}
GAL_PRICED = {"2eh": [{"product": "2EH", "um": "GAL", "cost": 6.90}]}


def fake_weight(name):
    return WEIGHTS.get(app_module.normalize_product_name(name))


class PrinterConversionTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(app_module, "get_product_weight", fake_weight)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_lb_to_gal_multiplies_by_weight(self):
        self.assertAlmostEqual(
            app_module.convert_printer_cost_between_ums(1.00, "LB", "GAL", "DDBSA"), 8.90
        )

    def test_gal_to_lb_divides_by_weight(self):
        self.assertAlmostEqual(
            app_module.convert_printer_cost_between_ums(6.90, "GAL", "LB", "2EH"), 1.00
        )

    def test_missing_weight_cannot_convert(self):
        self.assertIsNone(
            app_module.convert_printer_cost_between_ums(1.00, "LB", "GAL", "Unknown")
        )

    def test_default_letter_row_converts_cost_and_price(self):
        row = app_module._build_printer_row_from_default_letter_row(
            {"product": "DDBSA", "um": "GAL", "margin": 20}, LB_PRICED, 20
        )
        self.assertEqual(row["um"], "GAL")
        self.assertAlmostEqual(row["cost"], 8.90)
        self.assertAlmostEqual(row["final_price"], 10.68)
        self.assertEqual(row["source_um"], "LB")
        self.assertAlmostEqual(row["source_cost"], 1.00)

    def test_period_refresh_keeps_row_unit_and_converts(self):
        row = {"product": "DDBSA", "um": "GAL", "cost": 7.0, "margin": 20}
        result = app_module.recost_existing_printer_rows([row], LB_PRICED)[0]
        self.assertEqual(result["um"], "GAL")
        self.assertAlmostEqual(result["cost"], 8.90)
        self.assertAlmostEqual(result["final_price"], 10.68)
        self.assertEqual(result["source_um"], "LB")
        self.assertAlmostEqual(result["source_cost"], 1.00)

    def test_period_refresh_converts_gal_pricing_to_lb_row(self):
        row = {"product": "2EH", "um": "LB", "cost": 2.0, "margin": 0}
        result = app_module.recost_existing_printer_rows([row], GAL_PRICED)[0]
        self.assertEqual(result["um"], "LB")
        self.assertAlmostEqual(result["cost"], 1.00)

    def test_period_refresh_falls_back_when_no_weight(self):
        priced = {"unknown": [{"product": "Unknown", "um": "LB", "cost": 1.00}]}
        row = {"product": "Unknown", "um": "GAL", "cost": 7.0, "margin": 0}
        result = app_module.recost_existing_printer_rows([row], priced)[0]
        self.assertEqual(result["um"], "LB")
        self.assertAlmostEqual(result["cost"], 1.00)

    def test_forced_costs_keep_ship_and_pack(self):
        row = {
            "product": "DDBSA", "um": "GAL", "cost": 8.90, "margin": 20,
            "pre_shipping": 0.50, "pre_packaging": 0.25,
            "add_shipping": 0.10, "add_packaging": 0.05,
        }
        result = app_module.force_period_costs_on_rows([row], LB_PRICED)[0]
        self.assertAlmostEqual(result["final_cost"], 9.65)
        self.assertAlmostEqual(result["price"], 11.58)
        self.assertAlmostEqual(result["final_price"], 11.73)


class LiveCostTests(unittest.TestCase):
    """Every line's cost comes from the month's price and the product's weight."""

    def setUp(self):
        patcher = patch.object(app_module, "get_product_weight", fake_weight)
        patcher.start()
        self.addCleanup(patcher.stop)

    def apply(self, row, priced):
        return app_module.apply_live_cost_to_row(row, priced)

    def test_lb_price_on_gal_line_multiplies_by_weight(self):
        line = app_module.resolve_line_cost("DDBSA", "GAL", LB_PRICED)
        self.assertEqual(line["status"], "priced")
        self.assertEqual(line["um"], "GAL")
        self.assertAlmostEqual(line["cost"], 8.90)

    def test_gal_price_on_lb_line_divides_by_weight(self):
        line = app_module.resolve_line_cost("2EH", "LB", GAL_PRICED)
        self.assertEqual(line["um"], "LB")
        self.assertAlmostEqual(line["cost"], 1.00)

    def test_price_in_line_unit_is_used_instead_of_converting(self):
        priced = {"ddbsa": [
            {"product": "DDBSA", "um": "GAL", "cost": 9.50},
            {"product": "DDBSA", "um": "LB", "cost": 1.00},
        ]}
        self.assertAlmostEqual(app_module.resolve_line_cost("DDBSA", "GAL", priced)["cost"], 9.50)
        self.assertAlmostEqual(app_module.resolve_line_cost("DDBSA", "LB", priced)["cost"], 1.00)

    def test_no_weight_returns_cost_in_pricing_unit(self):
        priced = {"unknown": [{"product": "Unknown", "um": "LB", "cost": 1.00}]}
        line = app_module.resolve_line_cost("Unknown", "GAL", priced)
        self.assertEqual(line["status"], "cannot_convert")
        self.assertEqual(line["um"], "LB")
        self.assertAlmostEqual(line["cost"], 1.00)

    def test_stale_cost_is_replaced_and_repriced(self):
        row = {
            "product": "DDBSA", "um": "GAL", "cost": 7.0, "margin": 20, "price": 8.4,
            "pre_shipping": 0.50, "add_shipping": 0.10, "final_price": 8.5,
        }
        result = self.apply(row, LB_PRICED)
        self.assertAlmostEqual(result["cost"], 8.90)
        self.assertAlmostEqual(result["final_cost"], 9.40)
        self.assertAlmostEqual(result["price"], 11.28)
        self.assertAlmostEqual(result["final_price"], 11.38)
        self.assertAlmostEqual(result["pre_shipping"], 0.50)

    def test_typed_price_is_kept_when_cost_changes(self):
        row = {
            "product": "DDBSA", "um": "GAL", "cost": 7.0, "margin": 20,
            "price": 10.00, "price_locked": "1",
        }
        result = self.apply(row, LB_PRICED)
        self.assertAlmostEqual(result["cost"], 8.90)
        self.assertAlmostEqual(result["price"], 10.00)
        self.assertAlmostEqual(result["margin"], 12.3596, places=3)
        self.assertEqual(result["price_locked"], "1")

    def test_correct_cost_leaves_rounded_price_alone(self):
        row = {
            "product": "DDBSA", "um": "GAL", "cost": 8.90, "margin": 20,
            "price": 10.68, "final_price": 10.68, "final_cost": 8.90,
        }
        result = self.apply(row, LB_PRICED)
        self.assertAlmostEqual(result["price"], 10.68)
        self.assertAlmostEqual(result["final_price"], 10.68)

    def test_no_price_for_month_keeps_line_cost(self):
        row = {"product": "DDBSA", "um": "GAL", "cost": 7.0, "margin": 20, "price": 8.4}
        result = self.apply(row, {})
        self.assertAlmostEqual(result["cost"], 7.0)
        self.assertAlmostEqual(result["price"], 8.4)

    def test_inquire_line_gets_cost_without_a_price(self):
        row = {"product": "DDBSA", "um": "GAL", "cost": 7.0, "margin": 0, "is_inquire": "1"}
        result = self.apply(row, LB_PRICED)
        self.assertAlmostEqual(result["cost"], 8.90)
        self.assertTrue(result["is_inquire"])


class AddedProductRowTests(unittest.TestCase):
    """A product added to a list gets the month's price, then its Default U/M."""

    def setUp(self):
        patcher = patch.object(app_module, "get_product_weight", fake_weight)
        patcher.start()
        self.addCleanup(patcher.stop)

    def add(self, name, priced, default_um):
        products = [{"product": name, "default_um": default_um}]
        with patch.object(app_module, "load_company_products", lambda: products):
            return app_module._build_added_printer_row(name, priced, 20)

    def test_converts_month_price_to_default_um(self):
        row = self.add("DDBSA", LB_PRICED, "GAL")
        self.assertEqual(row["um"], "GAL")
        self.assertAlmostEqual(row["cost"], 8.90)
        self.assertAlmostEqual(row["final_price"], 10.68)
        self.assertEqual(row["source_um"], "LB")
        self.assertAlmostEqual(row["source_cost"], 1.00)

    def test_same_unit_is_not_converted(self):
        row = self.add("2EH", GAL_PRICED, "GAL")
        self.assertEqual(row["um"], "GAL")
        self.assertAlmostEqual(row["cost"], 6.90)
        self.assertAlmostEqual(row["final_price"], 8.28)

    def test_prefers_month_price_already_in_default_um(self):
        priced = {"ddbsa": [
            {"product": "DDBSA", "um": "GAL", "cost": 9.50},
            {"product": "DDBSA", "um": "LB", "cost": 1.00},
        ]}
        row = self.add("DDBSA", priced, "LB")
        self.assertEqual(row["um"], "LB")
        self.assertAlmostEqual(row["cost"], 1.00)

    def test_no_default_um_stays_in_pricing_unit(self):
        row = self.add("DDBSA", LB_PRICED, "")
        self.assertEqual(row["um"], "LB")
        self.assertAlmostEqual(row["cost"], 1.00)

    def test_no_weight_stays_in_pricing_unit(self):
        priced = {"unknown": [{"product": "Unknown", "um": "LB", "cost": 1.00}]}
        row = self.add("Unknown", priced, "GAL")
        self.assertEqual(row["um"], "LB")
        self.assertAlmostEqual(row["cost"], 1.00)


if __name__ == "__main__":
    unittest.main()
