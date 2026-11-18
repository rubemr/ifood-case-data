# Databricks notebook source
"""E11: médias horárias de passageiros, denominadores explícitos e sensibilidade aos zeros."""
import csv
import json
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/Workspace/Shared/ifood-case-data/src")
from case_config import load_case_config

config = load_case_config(dbutils)
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
run_id = uuid4().hex
view_name = config.table("gold", "yellow_trips")
silver_table = config.table("silver", "yellow_trips")
snapshot_version = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").first()["version"]
source = spark.read.option("versionAsOf", snapshot_version).table(silver_table).select(spark.table(view_name).columns)
blocks = [
    config.bind_sql("""-- E11: pickup local em maio; média principal por corrida inclui passageiros zero.
SELECT assert_true(COUNT(DISTINCT source_file_month) = 5, 'Esperadas cinco fontes'),
       assert_true(COUNT(CASE WHEN tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-05-01 00:00:00'
                              AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
                         THEN 1 END) > 0, 'Maio sem corridas')
FROM workspace.ifood_gold.yellow_trips;"""),
    config.bind_sql("""WITH horas AS (SELECT explode(sequence(0, 23)) AS hora), observado AS (
  SELECT HOUR(tpep_pickup_datetime) AS hora,
         SUM(passenger_count) AS passageiros,
         COUNT(*) AS corridas_elegiveis,
         AVG(CAST(passenger_count AS DECIMAL(20,6))) AS media_passageiros_precisa,
         ROUND(AVG(CAST(passenger_count AS DECIMAL(20,6))), 4) AS media_passageiros_por_corrida
  FROM workspace.ifood_gold.yellow_trips
  WHERE eligible_passengers
    AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-05-01 00:00:00'
    AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
  GROUP BY HOUR(tpep_pickup_datetime)
)
SELECT h.hora, COALESCE(o.passageiros, 0) AS passageiros,
       COALESCE(o.corridas_elegiveis, 0) AS corridas_elegiveis,
       o.media_passageiros_precisa, o.media_passageiros_por_corrida
FROM horas h LEFT JOIN observado o ON h.hora = o.hora
ORDER BY h.hora;"""),
    config.bind_sql("""WITH horas AS (SELECT explode(sequence(0, 23)) AS hora), observado AS (
  SELECT HOUR(tpep_pickup_datetime) AS hora, COUNT(*) AS corridas_observadas,
         COUNT(CASE WHEN eligible_passengers THEN 1 END) AS corridas_principal,
         COUNT(CASE WHEN eligible_passengers_positive THEN 1 END) AS corridas_positivas,
         COUNT(CASE WHEN array_contains(quality_reasons, 'flag_passenger_null') THEN 1 END) AS passageiros_nulos_origem,
         COUNT(CASE WHEN passenger_count = 0 THEN 1 END) AS passageiros_zero,
         COUNT(CASE WHEN passenger_count < 0 THEN 1 END) AS passageiros_negativos,
         COUNT(CASE WHEN SIZE(array_intersect(quality_reasons, ARRAY('flag_passenger_cast_failed', 'flag_passenger_fractional', 'flag_passenger_nonfinite'))) > 0 THEN 1 END) AS passageiros_invalidos,
         COUNT(CASE WHEN NOT eligible_passengers THEN 1 END) AS corridas_excluidas,
         AVG(CASE WHEN eligible_passengers THEN CAST(passenger_count AS DECIMAL(20,6)) END) AS media_principal_precisa,
         AVG(CASE WHEN eligible_passengers_positive THEN CAST(passenger_count AS DECIMAL(20,6)) END) AS media_positivos_precisa
  FROM workspace.ifood_gold.yellow_trips
  WHERE tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-05-01 00:00:00'
    AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
  GROUP BY HOUR(tpep_pickup_datetime)
)
SELECT h.hora,
       COALESCE(o.corridas_observadas, 0) AS corridas_observadas,
       COALESCE(o.corridas_principal, 0) AS corridas_principal,
       COALESCE(o.corridas_positivas, 0) AS corridas_positivas,
       COALESCE(o.passageiros_nulos_origem, 0) AS passageiros_nulos_origem,
       COALESCE(o.passageiros_zero, 0) AS passageiros_zero,
       COALESCE(o.passageiros_negativos, 0) AS passageiros_negativos,
       COALESCE(o.passageiros_invalidos, 0) AS passageiros_invalidos,
       COALESCE(o.corridas_excluidas, 0) AS corridas_excluidas,
       o.media_principal_precisa, o.media_positivos_precisa,
       ROUND(o.media_positivos_precisa, 4) AS media_apenas_positivos
FROM horas h LEFT JOIN observado o ON h.hora = o.hora
ORDER BY h.hora;"""),
    config.bind_sql("""SELECT COUNT(*) AS corridas_observadas,
       COUNT(CASE WHEN eligible_passengers THEN 1 END) AS corridas_elegiveis,
       COALESCE(SUM(CASE WHEN eligible_passengers THEN passenger_count END), 0) AS passageiros,
       COUNT(CASE WHEN eligible_passengers_positive THEN 1 END) AS corridas_positivas,
       COALESCE(SUM(CASE WHEN eligible_passengers_positive THEN passenger_count END), 0) AS passageiros_positivos,
       COUNT(CASE WHEN array_contains(quality_reasons, 'flag_passenger_null') THEN 1 END) AS passageiros_nulos_origem,
       COUNT(CASE WHEN passenger_count = 0 THEN 1 END) AS passageiros_zero,
       COUNT(CASE WHEN passenger_count < 0 THEN 1 END) AS passageiros_negativos,
       COUNT(CASE WHEN SIZE(array_intersect(quality_reasons, ARRAY('flag_passenger_cast_failed', 'flag_passenger_fractional', 'flag_passenger_nonfinite'))) > 0 THEN 1 END) AS passageiros_invalidos,
       COUNT(CASE WHEN NOT eligible_passengers THEN 1 END) AS corridas_excluidas,
       AVG(CASE WHEN eligible_passengers THEN CAST(passenger_count AS DECIMAL(20,6)) END) AS media_principal_precisa,
       AVG(CASE WHEN eligible_passengers_positive THEN CAST(passenger_count AS DECIMAL(20,6)) END) AS media_positivos_precisa
FROM workspace.ifood_gold.yellow_trips
WHERE tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-05-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00';""")
]
if len(blocks) != 4:
    raise ValueError("Expected four passenger SQL blocks")

def execute(index, pinned=True):
    sql = blocks[index].replace(view_name, "passenger_source") if pinned else blocks[index]
    return spark.sql(sql)

# Testa o SQL real com resultados esperados explícitos: hora ausente e média zero.
fixture_rows = [
    ("2023-05-01 00:00:00", 0, True, False, ["flag_passenger_zero"]),
    ("2023-05-02 00:05:00", 2, True, True, []),
    ("2023-05-03 00:10:00", None, False, False, ["flag_passenger_null"]),
    ("2023-05-01 01:00:00", 0, True, False, ["flag_passenger_zero"]),
    ("2023-06-01 00:00:00", 9, False, False, ["flag_pickup_outside_case"]),
    ("2023-04-30 23:59:59", 9, True, True, []),
    ("2023-05-31 23:59:59", 4, True, True, []),
    ("2023-05-01 01:10:00", -1, False, False, ["flag_passenger_negative"]),
    ("2023-05-01 02:00:00", None, False, False, ["flag_passenger_cast_failed"]),
]
fixture = spark.createDataFrame(fixture_rows, "tpep_pickup_datetime STRING, passenger_count INT, eligible_passengers BOOLEAN, eligible_passengers_positive BOOLEAN, quality_reasons ARRAY<STRING>")
fixture.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp_ntz")).createOrReplaceTempView("passenger_source")
observed = [row.asDict() for row in execute(1).collect()]
observed_quality = [row.asDict() for row in execute(2).collect()]
observed_period = execute(3).first().asDict()
assert [row["hora"] for row in observed] == list(range(24))
assert observed[0]["passageiros"] == 2 and observed[0]["corridas_elegiveis"] == 2
assert observed[0]["media_passageiros_por_corrida"] == Decimal("1.0000")
assert observed[1]["media_passageiros_por_corrida"] == Decimal("0.0000")
assert observed[2]["media_passageiros_por_corrida"] is None  # Corrida observada inválida.
assert observed[3]["media_passageiros_por_corrida"] is None  # Nenhuma corrida observada.
assert observed[23]["passageiros"] == 4 and observed[23]["corridas_elegiveis"] == 1
assert observed_quality[0]["media_apenas_positivos"] == Decimal("2.0000")
assert observed_quality[1]["media_apenas_positivos"] is None
assert observed_quality[2]["passageiros_invalidos"] == 1
assert observed_period["corridas_observadas"] == 7
assert observed_period["corridas_elegiveis"] == 4 and observed_period["passageiros"] == 6
assert observed_period["corridas_positivas"] == 2 and observed_period["corridas_excluidas"] == 3
assert observed_period["media_principal_precisa"] == Decimal("1.5000000000")

# COMMAND ----------

source.createOrReplaceTempView("passenger_source")
execute(0).collect()
hourly_frame, sensitivity_frame = execute(1), execute(2)
hourly = [row.asDict() for row in hourly_frame.collect()]
sensitivity = [row.asDict() for row in sensitivity_frame.collect()]
period_frame = execute(3)
period = period_frame.first().asDict()
if [row["hora"] for row in hourly] != list(range(24)) or [row["hora"] for row in sensitivity] != list(range(24)):
    raise AssertionError("Expected exactly 24 ordered hours")
if sum(row["corridas_elegiveis"] for row in hourly) != period["corridas_elegiveis"]:
    raise AssertionError("Hourly trip denominator does not reconcile")
if sum(row["passageiros"] for row in hourly) != period["passageiros"]:
    raise AssertionError("Hourly passenger numerator does not reconcile")
if sum(row["corridas_observadas"] for row in sensitivity) != period["corridas_observadas"]:
    raise AssertionError("Observed May population does not reconcile")
if period["corridas_observadas"] != period["corridas_elegiveis"] + period["corridas_excluidas"]:
    raise AssertionError("Eligible/excluded population mismatch")
for main, comparison in zip(hourly, sensitivity):
    assert main["hora"] == comparison["hora"]
    assert main["corridas_elegiveis"] == comparison["corridas_principal"]
    assert main["media_passageiros_precisa"] == comparison["media_principal_precisa"]
    assert comparison["corridas_principal"] == comparison["corridas_positivas"] + comparison["passageiros_zero"]
    if main["corridas_elegiveis"]:
        reference = Decimal(main["passageiros"]) / Decimal(main["corridas_elegiveis"])
        assert reference.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) == main["media_passageiros_precisa"]
    else:
        assert main["media_passageiros_precisa"] is None
for name in ("principal", "positivos"):
    numerator = period["passageiros"] if name == "principal" else period["passageiros_positivos"]
    denominator = period["corridas_elegiveis"] if name == "principal" else period["corridas_positivas"]
    reference = Decimal(numerator) / Decimal(denominator) if denominator else None
    assert period[f"media_{name}_precisa"] == (reference.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if reference is not None else None)
if [row.asDict() for row in execute(1, pinned=False).collect()] != hourly:
    raise AssertionError("Consumption view disagrees with the pinned source")

# COMMAND ----------

tables = [config.table("gold", "may_hourly_passenger_summary"),
          config.table("gold", "may_hourly_passenger_sensitivity"),
          config.table("gold", "may_passenger_period_summary")]
for frame, table, expected in zip((hourly_frame, sensitivity_frame, period_frame), tables, (24, 24, 1)):
    frame.write.format("delta").mode("overwrite").saveAsTable(table)
    if spark.table(table).count() != expected:
        raise AssertionError("Published passenger result shape mismatch")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.table('gold', 'results')}")
output = config.results_root / "passengers_may" / run_id
output.mkdir(parents=True, exist_ok=False)

def csv_export(name, rows):
    path = output / name
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return str(path)

exports = {"hourly": csv_export("passageiros_por_hora_maio.csv", hourly),
           "sensitivity": csv_export("sensibilidade_passageiros_maio.csv", sensitivity),
           "period": csv_export("resumo_passageiros_maio.csv", [period])}
result = {"stage": "E11", "status": "VERIFIED", "run_id": run_id,
          "executed_at": datetime.now(timezone.utc).isoformat(), "source_view": view_name,
          "silver_snapshot_version": snapshot_version, "spark_version": spark.version,
          "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
          "hourly": hourly, "sensitivity": sensitivity, "period": period,
          "fixture_rows_passed": len(fixture_rows), "hour_count": len(hourly),
          "hourly_period_reconciled": True, "live_view_reconciled": True,
          "result_tables": tables, "csv_exports": exports,
          "rules": ["May 2023 by local pickup; start inclusive, June 1 exclusive.",
                    "Main mean includes zero passengers and excludes null/invalid/negative values.",
                    "Sensitivity considers only positive passengers, with its own denominator.",
                    "No eligible observation means NULL average; observed zero-only trips mean zero average.",
                    "The period mean uses total passengers / eligible trips, not an unweighted mean of 24 hourly means."]}
encoded = json.dumps(result, default=str, ensure_ascii=False)
(spark.createDataFrame([(run_id, snapshot_version, result["executed_at"], encoded)],
                       "run_id STRING, silver_version LONG, executed_at STRING, evidence_json STRING")
 .write.format("delta").mode("append").saveAsTable(config.table("ops", "passenger_analysis_validation")))
(output.parent / "active.json").write_text(json.dumps({"run_id": run_id, "silver_version": snapshot_version, "exports": exports}, indent=2) + "\n", encoding="utf-8")
dbutils.notebook.exit(encoded)
