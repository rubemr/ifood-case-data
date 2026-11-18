"""Testes de contrato para validar fontes e selecionar versões imutáveis."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

spec = importlib.util.spec_from_file_location(
    "download_sources", Path(__file__).resolve().parents[1] / "src/download_sources.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def parquet_bytes(value=12.5):
    table = pa.table({"VendorID": [1], "passenger_count": [2.0],
                      "total_amount": [value],
                      "tpep_pickup_datetime": [None], "tpep_dropoff_datetime": [None]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


class Response(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.headers = {"Content-Length": str(len(data))}


class SourceTests(unittest.TestCase):
    def setUp(self):
        # Mantém os dados de teste no projeto para evitar MAX_PATH do Windows no TEMP do sandbox.
        Path('.tmp').mkdir(exist_ok=True)

    def test_reexecution_reuses_version_and_correction_preserves_both(self):
        with tempfile.TemporaryDirectory(dir='.tmp') as folder:
            root = Path(folder)
            with patch.object(module, "urlopen", side_effect=lambda *a, **kw: Response(parquet_bytes())):
                first = module.download_month("2023-01", root)
                second = module.download_month("2023-01", root)
            self.assertEqual(first["sha256"], second["sha256"])
            self.assertEqual(second["status"], "REUSED")
            with patch.object(module, "urlopen", return_value=Response(parquet_bytes(20.0))):
                third = module.download_month("2023-01", root)
            self.assertNotEqual(first["sha256"], third["sha256"])
            self.assertEqual(len(list(root.glob("2023-01/*/*.parquet"))), 2)
            active = json.loads((root / "2023-01/active.json").read_text())
            self.assertEqual(active["sha256"], third["sha256"])

    def test_invalid_parquet_does_not_activate_and_records_failure(self):
        with tempfile.TemporaryDirectory(dir='.tmp') as folder:
            root = Path(folder)
            with patch.object(module, "urlopen", return_value=Response(b"not a parquet")):
                with self.assertRaises(Exception):
                    module.download_month("2023-01", root)
            self.assertFalse((root / "2023-01/active.json").exists())
            self.assertFalse(list(root.glob("staging/*.partial")))
            event = json.loads(next((root / "runs").glob("*.json")).read_text())
            self.assertEqual(event["status"], "FAILED")

    def test_missing_column_is_rejected(self):
        with tempfile.TemporaryDirectory(dir='.tmp') as folder:
            path = Path(folder) / "missing.parquet"
            pq.write_table(pa.table({"VendorID": [1]}), path)
            with self.assertRaisesRegex(ValueError, "Missing required columns"):
                module.validate_parquet(path)


if __name__ == "__main__":
    unittest.main()
