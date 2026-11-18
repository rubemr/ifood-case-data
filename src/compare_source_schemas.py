"""Compara cada fonte mensal ativa antes de adicioná-la à Bronze."""
import json
from pathlib import Path


def main():
    root = Path("data/landing")
    sources = []
    baseline = None
    for month in (f"2023-{m:02d}" for m in range(1, 6)):
        active = json.loads((root / month / "active.json").read_text(encoding="utf-8"))
        manifest = json.loads((root / active["manifest_relative_path"]).read_text(encoding="utf-8"))
        schema = {column["name"]: column["type"] for column in manifest["columns"]}
        if baseline is None:
            baseline = schema
        difference = {
            key: {"january": baseline.get(key), "current": schema.get(key)}
            for key in sorted(set(baseline) | set(schema))
            if baseline.get(key) != schema.get(key)
        }
        sources.append({"source_file_month": month, "source_file": manifest["source_file"],
                        "source_url": manifest["source_url"], "sha256": manifest["sha256"],
                        "row_count": manifest["row_count"], "byte_count": manifest["byte_count"],
                        "column_count": len(schema), "differences_from_january": difference})
    # Permite explicitamente apenas as diferenças encontradas nas fontes de 2023.
    approved = {
        "VendorID": ("int64", "int32"), "PULocationID": ("int64", "int32"),
        "DOLocationID": ("int64", "int32"), "RatecodeID": ("double", "int64"),
        "passenger_count": ("double", "int64"), "store_and_fwd_flag": ("string", "large_string"),
        "airport_fee": ("double", None), "Airport_fee": (None, "double"),
    }
    for source in sources:
        source["differences_approved_for_runtime_roundtrip_check"] = all(
            approved.get(name) == (change["january"], change["current"])
            for name, change in source["differences_from_january"].items())
    report = {"stage": "E08", "schema_comparison": "name_and_type_per_column",
              "all_schemas_match": all(not source["differences_from_january"] for source in sources),
              "all_differences_explicitly_supported": all(source["differences_approved_for_runtime_roundtrip_check"] for source in sources),
              "total_rows": sum(source["row_count"] for source in sources),
              "total_bytes": sum(source["byte_count"] for source in sources),
              "sources": sources, "baseline_schema": baseline}
    target = Path("data/schema_comparison.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not report["all_differences_explicitly_supported"]:
        raise ValueError("Unapproved source schema difference; review before ingesting")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
