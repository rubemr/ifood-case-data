# Databricks notebook source
"""E10: executa o SQL financeiro versionado, reconcilia e publica os resultados reais."""
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
view_name = config.table("gold", "yellow_trips")
silver_table = config.table("silver", "yellow_trips")
run_id = uuid4().hex
snapshot_version = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").first()["version"]
view_columns = spark.table(view_name).columns
source = spark.read.option("versionAsOf", snapshot_version).table(silver_table).select(view_columns)
source.createOrReplaceTempView("finance_source")
blocks = [
    config.bind_sql("""-- E10: consultas executáveis no editor SQL sobre a view publicada.
-- Competência pelo pickup local; valores zero e negativos são preservados.
SELECT assert_true(
  COUNT(DISTINCT date_trunc('MONTH', tpep_pickup_datetime)) = 5,
  'Cobertura financeira incompleta: esperado janeiro a maio de 2023'
)
FROM workspace.ifood_gold.yellow_trips
WHERE eligible_amount
  AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00';"""),
    config.bind_sql("""SELECT CAST(date_trunc('MONTH', tpep_pickup_datetime) AS DATE) AS mes,
       COUNT(*) AS corridas_elegiveis,
       SUM(total_amount) AS total_reportado_usd,
       AVG(total_amount) AS ticket_medio_usd_preciso,
       ROUND(AVG(total_amount), 2) AS ticket_medio_usd
FROM workspace.ifood_gold.yellow_trips
WHERE eligible_amount
  AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
GROUP BY CAST(date_trunc('MONTH', tpep_pickup_datetime) AS DATE)
ORDER BY mes;"""),
    config.bind_sql("""WITH mensal AS (
  SELECT date_trunc('MONTH', tpep_pickup_datetime) AS mes,
         SUM(total_amount) AS total_mes
  FROM workspace.ifood_gold.yellow_trips
  WHERE eligible_amount
    AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
    AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
  GROUP BY date_trunc('MONTH', tpep_pickup_datetime)
)
SELECT COUNT(*) AS meses_observados,
       AVG(total_mes) AS media_totais_mensais_usd_precisa,
       ROUND(AVG(total_mes), 2) AS media_totais_mensais_usd
FROM mensal;"""),
    config.bind_sql("""SELECT COUNT(*) AS corridas_elegiveis,
       SUM(total_amount) AS total_periodo_usd,
       AVG(total_amount) AS ticket_periodo_usd_preciso,
       ROUND(AVG(total_amount), 2) AS ticket_periodo_usd,
       COUNT(CASE WHEN total_amount < 0 THEN 1 END) AS corridas_valor_negativo,
       COUNT(CASE WHEN total_amount = 0 THEN 1 END) AS corridas_valor_zero
FROM workspace.ifood_gold.yellow_trips
WHERE eligible_amount
  AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00';"""),
    config.bind_sql("""SELECT CAST(date_trunc('MONTH', tpep_pickup_datetime) AS DATE) AS mes,
       COUNT(CASE WHEN total_amount < 0 THEN 1 END) AS corridas_negativas,
       COALESCE(SUM(CASE WHEN total_amount < 0 THEN total_amount END), 0) AS contribuicao_negativa_usd,
       SUM(total_amount) AS total_principal_usd,
       COALESCE(SUM(CASE WHEN total_amount >= 0 THEN total_amount END), 0) AS total_sem_negativos_usd
FROM workspace.ifood_gold.yellow_trips
WHERE eligible_amount
  AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'
GROUP BY CAST(date_trunc('MONTH', tpep_pickup_datetime) AS DATE)
ORDER BY mes;""")
]
if len(blocks) != 5:
    raise ValueError("Expected five financial SQL blocks")

def execute(index, pinned=True):
    sql = blocks[index].replace(view_name, "finance_source") if pinned else blocks[index]
    return spark.sql(sql)

execute(0).collect()  # Mês ausente é erro de cobertura que bloqueia a execução, nunca receita zero.
monthly_frame = execute(1)
monthly = [row.asDict() for row in monthly_frame.collect()]
mean = execute(2).first().asDict()
period = execute(3).first().asDict()
sensitivity_frame = execute(4)
sensitivity = [row.asDict() for row in sensitivity_frame.collect()]
if [str(row["mes"]) for row in monthly] != [f"2023-{month:02d}-01" for month in range(1, 6)]:
    raise AssertionError("Unexpected monthly coverage")
if mean["meses_observados"] != 5:
    raise AssertionError("Expected five monthly totals")
if sum(row["corridas_elegiveis"] for row in monthly) != period["corridas_elegiveis"]:
    raise AssertionError("Monthly counts differ from the global count")
total = sum(row["total_reportado_usd"] for row in monthly)
if total != period["total_periodo_usd"]:
    raise AssertionError("Monthly totals differ from the period total")
if (total / Decimal(5)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) != mean["media_totais_mensais_usd"]:
    raise AssertionError("Monthly-total mean denominator or rounding mismatch")
weighted_ticket = total / Decimal(period["corridas_elegiveis"])
if weighted_ticket.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP) != period["ticket_periodo_usd_preciso"]:
    raise AssertionError("Global ticket is not weighted by eligible trips")
for item, comparison in zip(monthly, sensitivity):
    if item["mes"] != comparison["mes"] or item["total_reportado_usd"] != comparison["total_principal_usd"]:
        raise AssertionError("Sensitivity cohort differs from the main result")
    if comparison["total_principal_usd"] - comparison["contribuicao_negativa_usd"] != comparison["total_sem_negativos_usd"]:
        raise AssertionError("Negative-amount sensitivity does not reconcile")
# Confere se a view de consumo corresponde à projeção da versão fixada da Silver.
if [row.asDict() for row in execute(1, pinned=False).collect()] != monthly:
    raise AssertionError("Consumption view and pinned source disagree")
population = source.agg(F.count("*").alias("rows"),
                        F.sum(F.when(~F.col("eligible_amount"), 1).otherwise(0)).alias("ineligible_amount"),
                        F.sum(F.when(F.col("eligible_amount").isNull(), 1).otherwise(0)).alias("null_eligibility_flags")).first().asDict()
if population["null_eligibility_flags"] != 0 or population["rows"] != period["corridas_elegiveis"] + population["ineligible_amount"]:
    raise AssertionError("Eligible and ineligible financial populations do not reconcile")

# COMMAND ----------

monthly_table = config.table("gold", "monthly_financial_summary")
period_table = config.table("gold", "financial_period_summary")
sensitivity_table = config.table("gold", "monthly_financial_sensitivity")
monthly_frame.write.format("delta").mode("overwrite").saveAsTable(monthly_table)
sensitivity_frame.write.format("delta").mode("overwrite").saveAsTable(sensitivity_table)
period_frame = execute(2).crossJoin(execute(3))
period_frame.write.format("delta").mode("overwrite").saveAsTable(period_table)
if spark.table(monthly_table).count() != 5 or spark.table(period_table).count() != 1:
    raise AssertionError("Published result table shape mismatch")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.table('gold', 'results')}")
output = config.results_root / "financial" / run_id
output.mkdir(parents=True, exist_ok=False)

def csv_export(name, rows):
    path = output / name
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return str(path)

exports = {
    "monthly": csv_export("resultados_mensais.csv", monthly),
    "period": csv_export("media_totais_mensais.csv", [{**mean, **period}]),
    "sensitivity": csv_export("sensibilidade_financeira.csv", sensitivity),
}
result = {
    "stage": "E10", "status": "VERIFIED", "run_id": run_id,
    "executed_at": datetime.now(timezone.utc).isoformat(),
    "source_view": view_name, "silver_snapshot_version": snapshot_version,
    "session_timezone": spark.conf.get("spark.sql.session.timeZone"), "spark_version": spark.version,
    "monthly": monthly, "monthly_total_mean": mean, "period": period,
    "population": population, "negative_amount_sensitivity": sensitivity,
    "live_view_reconciled": True, "monthly_global_reconciled": True,
    "weighted_ticket_reconciled": True,
    "result_tables": [monthly_table, period_table, sensitivity_table], "csv_exports": exports,
    "rules": ["Calendar month from local pickup, start inclusive and end exclusive.",
              "Window: 2023-01-01 <= pickup < 2023-06-01.",
              "eligible_amount; zero and negative amounts included.",
              "total_amount in USD is the reported charge, not profit or verified net receipts.",
              "No per-vehicle average: VendorID is a provider identifier."],
}
encoded = json.dumps(result, default=str, ensure_ascii=False)
(spark.createDataFrame([(run_id, snapshot_version, result["executed_at"], encoded)],
                       "run_id STRING, silver_version LONG, executed_at STRING, evidence_json STRING")
 .write.format("delta").mode("append").saveAsTable(config.table("ops", "financial_analysis_validation")))
(output.parent / "active.json").write_text(
    json.dumps({"run_id": run_id, "silver_version": snapshot_version, "exports": exports}, indent=2) + "\n", encoding="utf-8")
dbutils.notebook.exit(encoded)
