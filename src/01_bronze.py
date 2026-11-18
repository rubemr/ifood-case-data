# Databricks notebook source
"""E05: lê uma fonte TLC ativa e substitui somente sua fatia na Bronze."""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/Workspace/Shared/ifood-case-data/src")
from case_config import load_case_config

config = load_case_config(dbutils)
dbutils.widgets.text("month", "2023-01")
catalog = config.catalog
month = dbutils.widgets.get("month")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog identifier")
if month not in {f"2023-{m:02d}" for m in range(1, 6)}:
    raise ValueError("Month outside the case scope")

root = config.landing_root
target = config.table("bronze", "yellow_trips")
audit_table = config.table("ops", "ingestion_manifest")
run_id = uuid4().hex
started_at = datetime.now(timezone.utc).isoformat()
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

active = json.loads((root / month / "active.json").read_text())
digest = active["sha256"]
if not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise ValueError("Invalid source digest")
filename = f"yellow_tripdata_{month}.parquet"
expected_file = f"{month}/{digest}/{filename}"
expected_manifest = f"{month}/{digest}/manifest.json"
if (active["file_relative_path"] != expected_file
        or active["manifest_relative_path"] != expected_manifest
        or active["source_file_month"] != month):
    raise ValueError("Active pointer does not match expected version layout")
source_path = root / expected_file
manifest = json.loads((root / expected_manifest).read_text())
if (manifest["sha256"] != digest or manifest["status"] != "VALIDATED"
        or manifest["source_file_month"] != month
        or manifest["source_file"] != filename):
    raise ValueError("Manifest identity/status mismatch")
hasher = hashlib.sha256()
with source_path.open("rb") as stream:
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        hasher.update(block)
if hasher.hexdigest() != digest or source_path.stat().st_size != manifest["byte_count"]:
    raise ValueError("Source bytes no longer match validated manifest")

source = spark.read.parquet(str(source_path))
original_schema = source.schema.jsonValue()
normalizations = []
# Janeiro define a estrutura existente da Bronze. As fontes posteriores usam int32/int64 e
# Airport_fee. Preserva arquivos e schemas originais e permite apenas casts sem perda de valores.
if spark.catalog.tableExists(target):
    technical = {"source_file", "source_file_month", "source_sha256", "source_url", "ingested_at", "run_id"}
    canonical = [field for field in spark.table(target).schema if field.name not in technical]
    input_names = set(source.columns)
    selected, checks = [], []
    mapped_names = set()
    for field in canonical:
        incoming = field.name
        if incoming == "airport_fee" and "airport_fee" not in input_names and "Airport_fee" in input_names:
            incoming = "Airport_fee"
        if incoming not in input_names:
            raise ValueError(f"Source column absent from canonical schema: {field.name}")
        mapped_names.add(incoming)
        incoming_type = source.schema[incoming].dataType.simpleString()
        canonical_type = field.dataType.simpleString()
        value = F.col(incoming)
        if incoming_type != canonical_type:
            converted = F.expr(f"try_cast(`{incoming}` AS {canonical_type})")
            returned = F.expr(f"try_cast(try_cast(`{incoming}` AS {canonical_type}) AS {incoming_type})")
            checks.append(F.sum(F.when(~returned.eqNullSafe(value), 1).otherwise(0)).alias(field.name))
            value = converted
        if incoming != field.name or incoming_type != canonical_type:
            normalizations.append({"incoming": incoming, "canonical": field.name,
                                   "incoming_type": incoming_type, "canonical_type": canonical_type})
        selected.append(value.alias(field.name))
    if mapped_names != input_names:
        raise ValueError(f"Unexpected source columns: {sorted(input_names - mapped_names)}")
    if checks:
        violations = source.agg(*checks).first().asDict()
        if any(value != 0 for value in violations.values()):
            raise ValueError(f"Lossy source normalization: {violations}")
    source = source.select(*selected)
required = {"VendorID", "passenger_count", "total_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime"}
if not required.issubset(source.columns):
    raise ValueError(f"Required columns missing: {sorted(required - set(source.columns))}")
# Não reinterpreta horários locais como UTC. Interrompe a execução se houver mudança silenciosa.
for field in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
    if source.schema[field].dataType.simpleString() != "timestamp_ntz":
        raise ValueError(f"Expected timestamp_ntz for {field}; inspect Parquet mapping before proceeding")
source_columns = source.columns


def metrics(frame):
    """Compara contagens, total monetário de controle e soma de hashes de todos os campos de origem.

    A soma de hashes é uma reconciliação probabilística adicional, não uma chave primária.
    O cast monetário para decimal serve apenas ao controle, sem mudar o tipo na Bronze.
    """
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("total_amount").cast("decimal(30,6)")).alias("amount_control"),
        F.sum(F.xxhash64(*[F.col(c) for c in source_columns])
              .cast("decimal(38,0)")).alias("row_hash_sum"),
    ).first()
    return {key: str(value) if value is not None else None for key, value in row.asDict().items()}


source_metrics = metrics(source)
if int(source_metrics["rows"]) != manifest["row_count"] or manifest["row_count"] <= 0:
    raise ValueError("Spark source count and manifest disagree or source is empty")

# COMMAND ----------

predicate = F.col("source_file_month") == month
table_existed = spark.catalog.tableExists(target)
previous_metrics = None
other_months_before = 0
if table_existed:
    previous = spark.table(target)
    if previous.select(source_columns).schema != source.schema:
        raise ValueError("Source schema differs from existing Bronze; explicit evolution required")
    previous_metrics = metrics(previous.where(predicate))
    other_months_before = previous.where(~predicate | F.col("source_file_month").isNull()).count()

bronze = (source
    .withColumn("source_file", F.lit(filename))
    .withColumn("source_file_month", F.lit(month))
    .withColumn("source_sha256", F.lit(digest))
    .withColumn("source_url", F.lit(manifest["source_url"]))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("run_id", F.lit(run_id)))

if table_existed:
    (bronze.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"source_file_month = '{month}'").saveAsTable(target))
else:
    bronze.write.format("delta").mode("error").saveAsTable(target)

persisted = spark.table(target)
after = persisted.where(predicate)
after_metrics = metrics(after)
if source_metrics != after_metrics:
    raise AssertionError(f"Source/Bronze mismatch: {source_metrics} != {after_metrics}")
other_months_after = persisted.where(~predicate | F.col("source_file_month").isNull()).count()
if other_months_before != other_months_after:
    raise AssertionError("Rows outside the replaced source month changed")
metadata_mismatches = after.where(
    ~F.col("source_sha256").eqNullSafe(F.lit(digest))
    | ~F.col("source_file").eqNullSafe(F.lit(filename))
    | ~F.col("run_id").eqNullSafe(F.lit(run_id))
    | F.col("ingested_at").isNull()
).count()
if metadata_mismatches:
    raise AssertionError("Bronze provenance metadata mismatch")

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()
result = {
    "stage": "E05", "status": "VERIFIED", "run_id": run_id,
    "started_at": started_at, "verified_at": datetime.now(timezone.utc).isoformat(),
    "table": target, "source_file_month": month, "source_sha256": digest,
    "source": source_metrics, "bronze": after_metrics, "previous": previous_metrics,
    "same_content_as_previous": previous_metrics == after_metrics if table_existed else None,
    "other_months_before": other_months_before, "other_months_after": other_months_after,
    "metadata_mismatches": metadata_mismatches, "source_columns": len(source_columns),
    "bronze_columns": len(persisted.columns), "delta_version": history["version"],
    "spark_version": spark.version, "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "source_schema": source.schema.jsonValue(),
    "original_source_schema": original_schema, "schema_normalizations": normalizations,
    "normalization_roundtrip_violations": 0,
    "publication": "source_file_month replacement; values preserved by roundtrip-checked normalization",
}
# A auditoria é incremental: duas tentativas bem-sucedidas geram duas linhas de auditoria, sem duplicar corridas.
audit = [(run_id, month, digest, result["verified_at"], "VERIFIED", json.dumps(result))]
(spark.createDataFrame(audit, "run_id STRING, source_file_month STRING, source_sha256 STRING, "
                       "verified_at STRING, status STRING, evidence_json STRING")
 .write.format("delta").mode("append").saveAsTable(audit_table))
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
