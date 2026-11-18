# Databricks notebook source
"""E07: Silver tipada, flags de qualidade auditadas, testes sintéticos e view SQL."""
import json
import re
from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/Workspace/Shared/ifood-case-data/src")
from case_config import load_case_config

config = load_case_config(dbutils)
catalog = config.catalog
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
contract_version = "v2-zero-included"
silver_run_id = uuid4().hex
required = ["VendorID", "passenger_count", "total_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime"]
bronze_table = config.table("bronze", "yellow_trips")
silver_table = config.table("silver", "yellow_trips")
gold_view = config.table("gold", "yellow_trips")


def transform(frame):
    out = frame
    for name in required:
        out = out.withColumn(f"raw_{name}", F.col(name))
    pd = F.expr("try_cast(raw_passenger_count AS DOUBLE)")
    pi = F.expr("try_cast(raw_passenger_count AS INT)")
    ad = F.expr("try_cast(raw_total_amount AS DOUBLE)")
    amount = F.expr("try_cast(raw_total_amount AS DECIMAL(18,2))")
    vd = F.expr("try_cast(raw_VendorID AS DOUBLE)")
    vi = F.expr("try_cast(raw_VendorID AS INT)")

    def finite(value):
        return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))

    passenger_valid = finite(pd) & pi.isNotNull() & (pd == pi.cast("double"))
    vendor_valid = finite(vd) & vi.isNotNull() & (vd == vi.cast("double"))
    amount_valid = finite(ad) & amount.isNotNull()
    out = (out.withColumn("VendorID", F.when(vendor_valid, vi).cast("int"))
           .withColumn("passenger_count", F.when(passenger_valid, pi).cast("int"))
           .withColumn("total_amount", F.when(amount_valid, amount).cast("decimal(18,2)"))
           .withColumn("tpep_pickup_datetime", F.expr("try_cast(raw_tpep_pickup_datetime AS TIMESTAMP_NTZ)"))
           .withColumn("tpep_dropoff_datetime", F.expr("try_cast(raw_tpep_dropoff_datetime AS TIMESTAMP_NTZ)")))
    pick, drop = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    in_window = ((pick >= F.lit("2023-01-01").cast("timestamp_ntz"))
                 & (pick < F.lit("2023-06-01").cast("timestamp_ntz")))
    source_start = F.expr("try_cast(concat(source_file_month, '-01') AS TIMESTAMP_NTZ)")
    source_end = F.add_months(source_start.cast("date"), 1).cast("timestamp_ntz")
    flags = {
        "flag_vendor_null": F.col("raw_VendorID").isNull(),
        "flag_vendor_invalid": F.col("raw_VendorID").isNotNull() & ~F.coalesce(vendor_valid, F.lit(False)),
        "flag_passenger_null": F.col("raw_passenger_count").isNull(),
        "flag_passenger_zero": F.col("passenger_count") == 0,
        "flag_passenger_negative": pd < 0,
        "flag_passenger_fractional": finite(pd) & (pd != F.floor(pd)),
        "flag_passenger_nonfinite": pd.isNotNull() & ~finite(pd),
        "flag_passenger_cast_failed": F.col("raw_passenger_count").isNotNull() & pi.isNull(),
        "flag_amount_null": F.col("raw_total_amount").isNull(),
        "flag_amount_zero": F.col("total_amount") == 0,
        "flag_amount_negative": F.col("total_amount") < 0,
        "flag_amount_nonfinite": ad.isNotNull() & ~finite(ad),
        "flag_amount_cast_failed": F.col("raw_total_amount").isNotNull() & amount.isNull(),
        "flag_amount_rounded": amount_valid & (F.abs(ad - amount.cast("double")) > 1e-9),
        "flag_pickup_null": F.col("raw_tpep_pickup_datetime").isNull(),
        "flag_pickup_cast_failed": F.col("raw_tpep_pickup_datetime").isNotNull() & pick.isNull(),
        "flag_dropoff_null": F.col("raw_tpep_dropoff_datetime").isNull(),
        "flag_dropoff_cast_failed": F.col("raw_tpep_dropoff_datetime").isNotNull() & drop.isNull(),
        "flag_pickup_outside_case": pick.isNotNull() & ~in_window,
        "flag_pickup_outside_source_month": pick.isNotNull() & ((pick < source_start) | (pick >= source_end)),
        "flag_dropoff_before_pickup": drop < pick,
        "flag_duration_zero": drop == pick,
    }
    for name, expression in flags.items():
        out = out.withColumn(name, F.coalesce(expression, F.lit(False)))
    out = (out.withColumn("pickup_in_case_window", F.coalesce(in_window, F.lit(False)))
           .withColumn("pickup_month", F.date_trunc("month", pick).cast("date"))
           .withColumn("pickup_hour", F.hour(pick))
           .withColumn("duration_seconds", F.expr("timestampdiff(MICROSECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 1000000.0"))
           .withColumn("eligible_amount", F.col("pickup_in_case_window") & F.col("total_amount").isNotNull())
           .withColumn("eligible_passengers", F.col("pickup_in_case_window") & F.col("passenger_count").isNotNull() & (F.col("passenger_count") >= 0))
           .withColumn("eligible_passengers_positive", F.col("pickup_in_case_window") & F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0))
           .withColumn("quality_reasons", F.filter(F.array(*[F.when(F.col(name), F.lit(name)) for name in flags]), lambda value: value.isNotNull()))
           .withColumn("quality_contract_version", F.lit(contract_version))
           .withColumn("silver_run_id", F.lit(silver_run_id))
           .withColumn("silver_processed_at", F.current_timestamp()))
    return out, list(flags)

# COMMAND ----------

# Resultados esperados independentes para limites, ausências, valores malformados e duplicados.
cases = [
    ("start", "2", "10", "2023-01-01 00:00:00", "2023-01-01 00:10:00"),
    ("zero", "0", "0", "2023-01-01 23:00:00", "2023-01-01 23:10:00"),
    ("null", None, "-5", "2023-05-31 23:59:59", "2023-06-01 00:10:00"),
    ("negative", "-1", "-10", "2023-01-15 12:00:00", "2023-01-15 12:10:00"),
    ("fraction", "1.5", "1.005", "2023-01-15 12:00:00", "2023-01-15 12:10:00"),
    ("invalid", "oops", "oops", "invalid", "invalid"),
    ("before", "1", "1", "2022-12-31 23:59:59", "2023-01-01 00:10:00"),
    ("end", "1", "1", "2023-06-01 00:00:00", "2023-06-01 00:10:00"),
    ("may", "1", "1", "2023-05-01 00:00:00", "2023-05-01 00:10:00"),
    ("dst", "1", "1", "2023-03-12 01:59:00", "2023-03-12 03:01:00"),
    ("dropoff", "3", "3", "2023-01-01 01:00:00", "2023-01-01 00:59:00"),
    ("duplicate1", "2", "10", "2023-01-02 12:00:00", "2023-01-02 12:10:00"),
    ("duplicate2", "2", "10", "2023-01-02 12:00:00", "2023-01-02 12:10:00"),
    ("nonfinite", "NaN", "Infinity", "2023-01-01 12:00:00", "2023-01-01 12:10:00"),
    ("overflow", "2147483648", "1", "2023-01-01 12:00:00", "2023-01-01 12:10:00"),
]
fixture = spark.createDataFrame(cases, "fixture_id STRING, passenger_count STRING, total_amount STRING, tpep_pickup_datetime STRING, tpep_dropoff_datetime STRING")
fixture = fixture.withColumn("VendorID", F.lit("1")).withColumn("source_file_month", F.lit("2023-01"))
tested, _ = transform(fixture)
observed = {r["fixture_id"]: r.asDict() for r in tested.collect()}
assert len(observed) == 15
for name in ("start", "zero", "may", "dst", "dropoff", "duplicate1", "duplicate2"):
    assert observed[name]["eligible_passengers"] is True, name
for name in ("null", "negative", "fraction", "invalid", "before", "end", "nonfinite", "overflow"):
    assert observed[name]["eligible_passengers"] is False, name
assert observed["zero"]["passenger_count"] == 0 and not observed["zero"]["eligible_passengers_positive"]
assert observed["null"]["passenger_count"] is None and observed["null"]["eligible_amount"]
assert observed["negative"]["eligible_amount"] and observed["negative"]["flag_amount_negative"]
assert observed["fraction"]["passenger_count"] is None and observed["fraction"]["flag_passenger_fractional"]
assert str(observed["fraction"]["total_amount"]) == "1.01" and observed["fraction"]["flag_amount_rounded"]
assert observed["invalid"]["flag_pickup_cast_failed"] and not observed["invalid"]["eligible_amount"]
assert observed["nonfinite"]["flag_passenger_nonfinite"] and not observed["nonfinite"]["eligible_amount"]
assert observed["overflow"]["flag_passenger_cast_failed"]
assert observed["start"]["tpep_pickup_datetime"] == datetime(2023, 1, 1)
assert observed["zero"]["pickup_hour"] == 23 and observed["may"]["pickup_hour"] == 0
assert observed["dst"]["pickup_hour"] == 1  # Preserva o horário local, sem conversão para UTC.
assert observed["dropoff"]["flag_dropoff_before_pickup"] and observed["dropoff"]["eligible_amount"]

# COMMAND ----------

bronze_version = spark.sql(f"DESCRIBE HISTORY {bronze_table} LIMIT 1").first()["version"]
source = spark.read.option("versionAsOf", bronze_version).table(bronze_table)
for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
    if source.schema[name].dataType.simpleString() != "timestamp_ntz":
        raise ValueError("Inspect timestamp mapping before transforming this source")
source_columns = source.columns
silver, flag_names = transform(source)


def reconcile(frame):
    row = frame.agg(F.count("*").alias("rows"),
                    F.sum(F.xxhash64(*[F.col(c) for c in source_columns]).cast("decimal(38,0)")).alias("all_bronze_fields_hash_sum")).first()
    return {key: str(value) for key, value in row.asDict().items()}


def restore_bronze(frame):
    return frame.select(*[F.col(f"raw_{c}" if c in required else c).alias(c) for c in source_columns])


source_control = reconcile(source)
if int(source_control["rows"]) <= 0:
    raise ValueError("Refuse empty Silver rebuild")
existing = spark.catalog.tableExists(silver_table)
previous_control = reconcile(restore_bronze(spark.table(silver_table))) if existing else None
# Reconstrói a partir de todas as fontes atuais da Bronze, além do mês de pickup.
silver.write.format("delta").mode("overwrite" if existing else "error").saveAsTable(silver_table)
persisted = spark.table(silver_table)
after_control = reconcile(restore_bronze(persisted))
if source_control != after_control:
    raise AssertionError("Source row count or original-field control changed")

# COMMAND ----------

def n(condition, alias):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(alias)


summary = persisted.agg(
    F.count("*").alias("rows"),
    n(F.col("eligible_amount"), "eligible_amount"), n(~F.col("eligible_amount"), "ineligible_amount"),
    n(F.col("eligible_passengers"), "eligible_passengers"), n(~F.col("eligible_passengers"), "ineligible_passengers"),
    n(F.col("eligible_passengers_positive"), "eligible_passengers_positive"),
    *[n(F.col(name), name) for name in flag_names],
).first().asDict()
for metric in ("amount", "passengers"):
    assert summary[f"eligible_{metric}"] + summary[f"ineligible_{metric}"] == summary["rows"]
assert persisted.where(F.col("eligible_passengers") & F.col("passenger_count").isNull()).count() == 0

view_columns = required + ["eligible_amount", "eligible_passengers", "eligible_passengers_positive",
    "pickup_month", "pickup_hour", "quality_reasons", "quality_contract_version",
    "source_file", "source_file_month", "source_sha256"]
spark.sql(f"CREATE OR REPLACE VIEW {gold_view} AS SELECT {', '.join(view_columns)} FROM {silver_table}")
view_rows = spark.table(gold_view).count()
assert view_rows == summary["rows"]
by_source = [r.asDict() for r in persisted.groupBy("source_file_month", "source_sha256").count().collect()]
result = {
    "stage": "E07", "status": "VERIFIED", "silver_run_id": silver_run_id,
    "verified_at": datetime.now(timezone.utc).isoformat(), "contract_version": contract_version,
    "bronze_table": bronze_table, "bronze_version": bronze_version,
    "silver_table": silver_table, "silver_version": spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").first()["version"],
    "gold_view": gold_view, "view_rows": view_rows,
    "source_control": source_control, "silver_restored_control": after_control,
    "previous_control": previous_control, "same_original_content_as_previous": previous_control == after_control if existing else None,
    "summary": summary, "by_source": by_source,
    "silver_columns": len(persisted.columns), "view_columns": len(view_columns),
    "required_types": {name: persisted.schema[name].dataType.simpleString() for name in required},
    "fixture_cases_passed": len(cases), "spark_version": spark.version,
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
}
encoded = json.dumps(result, default=str)
(spark.createDataFrame([(silver_run_id, contract_version, result["verified_at"], encoded)],
                       "run_id STRING, contract_version STRING, verified_at STRING, evidence_json STRING")
 .write.format("delta").mode("append").saveAsTable(config.table("ops", "silver_validation")))
dbutils.notebook.exit(encoded)
