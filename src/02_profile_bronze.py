# Databricks notebook source
"""E06: explora a Bronze e mede a elegibilidade proposta sem alterar corridas."""
import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/Workspace/Shared/ifood-case-data/src")
from case_config import load_case_config

config = load_case_config(dbutils)
dbutils.widgets.text("month", "2023-01")
catalog, month = config.catalog, dbutils.widgets.get("month")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Invalid catalog")
if month not in {f"2023-{m:02d}" for m in range(1, 6)}:
    raise ValueError("Month outside scope")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
table = config.table("bronze", "yellow_trips")
run_id = uuid4().hex
snapshot_version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
base = (spark.read.option("versionAsOf", snapshot_version).table(table)
        .where(F.col("source_file_month") == month))
required = ["VendorID", "passenger_count", "total_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime"]

# COMMAND ----------

def count_if(condition, name):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(name)


def finite(name):
    value = F.col(name)
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != F.lit(float("inf")))


p = F.col("passenger_count")
a = F.col("total_amount")
pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
in_scope = ((pickup >= F.lit("2023-01-01").cast("timestamp_ntz"))
            & (pickup < F.lit("2023-06-01").cast("timestamp_ntz")))
month_end = f"2023-{int(month[-2:]) + 1:02d}-01"
in_month = ((pickup >= F.lit(f"{month}-01").cast("timestamp_ntz"))
            & (pickup < F.lit(month_end).cast("timestamp_ntz")))
passenger_int = F.expr("try_cast(passenger_count AS INT)")
amount_decimal = F.expr("try_cast(total_amount AS DECIMAL(18,2))")
valid_passenger = finite("passenger_count") & passenger_int.isNotNull() & (p == passenger_int)
valid_amount = finite("total_amount") & amount_decimal.isNotNull()
eligible_p = F.coalesce(in_scope & valid_passenger & (p >= 0), F.lit(False))
eligible_positive = F.coalesce(in_scope & valid_passenger & (p > 0), F.lit(False))
eligible_a = F.coalesce(in_scope & valid_amount, F.lit(False))
duration = F.expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)")
flags = {
    "passenger_null": p.isNull(), "passenger_zero": p == 0,
    "passenger_negative": p < 0,
    "passenger_nonfinite": p.isNotNull() & ~finite("passenger_count"),
    "passenger_cast_failed": p.isNotNull() & passenger_int.isNull(),
    "passenger_fractional": finite("passenger_count") & passenger_int.isNotNull() & (p != passenger_int),
    "amount_null": a.isNull(), "amount_zero": a == 0,
    "amount_negative": a < 0,
    "amount_nonfinite": a.isNotNull() & ~finite("total_amount"),
    "amount_cast_failed": a.isNotNull() & amount_decimal.isNull(),
    "amount_rounding_over_1e_9": valid_amount & (F.abs(a - amount_decimal.cast("double")) > 1e-9),
    "pickup_null": pickup.isNull(), "dropoff_null": dropoff.isNull(),
    "pickup_outside_source_month": pickup.isNotNull() & ~in_month,
    "pickup_outside_case_window": pickup.isNotNull() & ~in_scope,
    "dropoff_before_pickup": duration < 0,
    "duration_zero": duration == 0,
    "duration_over_24h_diagnostic_only": duration > 86400,
}
summary_row = base.agg(
    F.count("*").alias("rows"),
    *[count_if(F.col(c).isNull(), f"null_{c}") for c in required],
    *[count_if(condition, name) for name, condition in flags.items()],
    count_if(eligible_a, "eligible_amount"), count_if(~eligible_a, "ineligible_amount"),
    count_if(eligible_p, "eligible_passengers"), count_if(~eligible_p, "ineligible_passengers"),
    count_if(eligible_positive, "eligible_passengers_positive"),
    F.min(pickup).alias("pickup_min"), F.max(pickup).alias("pickup_max"),
    F.min(dropoff).alias("dropoff_min"), F.max(dropoff).alias("dropoff_max"),
    F.min(F.when(finite("passenger_count"), p)).alias("passenger_min"),
    F.max(F.when(finite("passenger_count"), p)).alias("passenger_max"),
    F.min(F.when(finite("total_amount"), a)).alias("amount_min"),
    F.max(F.when(finite("total_amount"), a)).alias("amount_max"),
    F.percentile_approx(F.when(finite("total_amount"), a), [0.5, 0.95, 0.99], 10000).alias("amount_percentiles_p50_p95_p99"),
    F.percentile_approx(F.when(duration > 0, duration), [0.5, 0.95, 0.99], 10000).alias("positive_duration_seconds_p50_p95_p99"),
).first().asDict()
if summary_row["rows"] == 0:
    raise ValueError("No Bronze rows for the selected source month")
for metric in ("amount", "passengers"):
    if summary_row[f"eligible_{metric}"] + summary_row[f"ineligible_{metric}"] != summary_row["rows"]:
        raise AssertionError(f"Eligibility reconciliation failed: {metric}")

# COMMAND ----------

# A sensibilidade por mês de calendário difere da população do arquivo de origem inteiro.
calendar = base.where(in_month)
sensitivity = calendar.agg(
    F.count("*").alias("calendar_month_rows"),
    count_if(valid_passenger & (p > 0), "positive_passenger_trips"),
    count_if(valid_passenger & (p >= 0), "nonnegative_passenger_trips"),
    F.avg(F.when(valid_passenger & (p > 0), p)).alias("passenger_mean_excluding_zero"),
    F.avg(F.when(valid_passenger & (p >= 0), p)).alias("passenger_mean_including_zero"),
    F.sum(F.when(valid_amount, amount_decimal)).alias("amount_sum_including_negative"),
    F.sum(F.when(valid_amount & (a >= 0), amount_decimal)).alias("amount_sum_excluding_negative"),
    F.sum(F.when(valid_amount & (a < 0), amount_decimal)).alias("negative_amount_sum"),
    count_if(valid_amount & (a < 0), "negative_amount_trips"),
).first().asDict()
duplicate_groups = base.groupBy(required).count().where(F.col("count") > 1)
duplicates = duplicate_groups.agg(
    F.count("*").alias("groups_equal_on_five_fields"),
    F.coalesce(F.sum("count"), F.lit(0)).alias("rows_in_groups"),
    F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("excess_if_five_fields_were_deduplicated"),
).first().asDict()
vendor_distribution = [r.asDict() for r in base.groupBy("VendorID").count().orderBy(F.col("VendorID").asc_nulls_first()).collect()]
passenger_distribution = [r.asDict() for r in base.groupBy("passenger_count").count().orderBy(F.col("passenger_count").asc_nulls_first()).limit(50).collect()]
pickup_months = [r.asDict() for r in base.groupBy(F.date_format(pickup, "yyyy-MM").alias("pickup_month")).count().orderBy(F.col("pickup_month").asc_nulls_first()).collect()]
sample_flags = ["passenger_null", "passenger_zero", "amount_negative", "pickup_outside_source_month", "dropoff_before_pickup"]
samples = {
    name: [r.asDict() for r in base.where(flags[name]).select(required)
           .orderBy(*[F.col(c).asc_nulls_first() for c in required]).limit(3).collect()]
    for name in sample_flags
}

# COMMAND ----------

result = {
    "stage": "E06", "status": "PROFILED", "run_id": run_id,
    "profiled_at": datetime.now(timezone.utc).isoformat(),
    "source_table": table, "source_delta_version": snapshot_version,
    "source_file_month": month,
    "source_sha256": [r[0] for r in base.select("source_sha256").distinct().collect()],
    "spark_version": spark.version, "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
    "summary": summary_row, "calendar_month_sensitivity": sensitivity,
    "apparent_duplicates": duplicates, "vendor_distribution": vendor_distribution,
    "passenger_distribution_first_50": passenger_distribution,
    "pickup_month_distribution": pickup_months, "examples_up_to_three_per_flag": samples,
    "source_modified": False,
    "notes": ["Flag counts can overlap; do not sum them as exclusive rejection reasons.",
              "This reviewed profiler uses nonnegative reported passengers for eligible_passengers; the historical E06 JSON used positive-only and is preserved unchanged.",
              "Five-field equality is not proof that two reported trips are duplicates.",
              "Duration over 24h is diagnostic, not an exclusion rule.",
              "NULL is not imputed; reported zeros remain ambiguous and are also shown in positive-only sensitivity.",
              "January sensitivity does not answer the May hourly question."],
}
encoded = json.dumps(result, default=str, ensure_ascii=False)
# Persiste um relatório por mês de origem e execução; a Bronze permanece intacta.
record = [(run_id, month, snapshot_version, result["profiled_at"], encoded)]
(spark.createDataFrame(record, "run_id STRING, source_file_month STRING, source_delta_version LONG, profiled_at STRING, report_json STRING")
 .write.format("delta").mode("append").saveAsTable(config.table("ops", "quality_metrics")))
dbutils.notebook.exit(encoded)
