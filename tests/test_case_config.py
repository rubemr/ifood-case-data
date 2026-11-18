"""Verifica o isolamento dos namespaces e a rejeição de identificadores fora do case."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from case_config import CaseConfig


class ConfigTests(unittest.TestCase):
    def test_all_writes_are_isolated_while_landing_is_shared(self):
        main = CaseConfig()
        isolated = CaseConfig(schema_prefix="ifood_e13_123")
        for layer in ("bronze", "silver", "gold", "ops"):
            self.assertNotEqual(main.schema(layer), isolated.schema(layer))
            self.assertTrue(isolated.table(layer, "yellow_trips").startswith("workspace.ifood_e13_123_"))
        self.assertEqual(main.landing_root, isolated.landing_root)
        self.assertNotEqual(main.results_root, isolated.results_root)
        self.assertEqual(isolated.bind_sql("SELECT * FROM workspace.ifood_gold.yellow_trips"),
                         "SELECT * FROM workspace.ifood_e13_123_gold.yellow_trips")

    def test_unsafe_or_unrelated_identifiers_are_rejected(self):
        for prefix in ("unrelated", "ifood; DROP SCHEMA x", "ifood_e13_../main"):
            with self.assertRaises(ValueError):
                CaseConfig(schema_prefix=prefix)
        with self.assertRaises(ValueError):
            CaseConfig().table("gold", "bad.name")


if __name__ == "__main__":
    unittest.main()
