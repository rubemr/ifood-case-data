"""Baixa seis CSVs agregados validados de tarefas bem-sucedidas."""
import argparse
import csv
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]

def cli(*args):
    return subprocess.check_output(["databricks", *args, "--profile", "ifood-case", "--output", "json"], text=True, encoding="utf-8")

def evidence(task_id):
    result = json.loads(cli("jobs", "get-run-output", str(task_id)))
    if result["metadata"]["state"].get("result_state") != "SUCCESS" or result.get("notebook_output", {}).get("truncated"):
        raise ValueError("Task must have a complete successful notebook result")
    value = json.loads(result["notebook_output"]["result"])
    if value["status"] != "VERIFIED":
        raise ValueError("Analysis was not verified")
    return value

def equal(actual, expected):
    if expected is None:
        return actual == ""
    if isinstance(expected, (int, float, Decimal)):
        return Decimal(actual) == Decimal(str(expected))
    try:
        return Decimal(actual) == Decimal(str(expected))
    except InvalidOperation:
        return actual == str(expected)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--financial-task", type=int)
    parser.add_argument("--passenger-task", type=int)
    args = parser.parse_args()
    if args.run_id:
        if args.financial_task or args.passenger_task:
            parser.error("Use either a workflow run or the two task IDs")
        run = json.loads(cli("jobs", "get-run", str(args.run_id)))
        if run["state"].get("result_state") != "SUCCESS":
            raise ValueError("Workflow must have completed successfully")
        tasks = {x["task_key"]: x["run_id"] for x in run["tasks"]}
        args.financial_task, args.passenger_task = tasks["financial_analysis"], tasks["passenger_analysis"]
    elif not (args.financial_task and args.passenger_task):
        parser.error("Supply --run-id or both task IDs")
    finance, people = evidence(args.financial_task), evidence(args.passenger_task)
    if finance["stage"] != "E10" or people["stage"] != "E11" or finance["silver_snapshot_version"] != people["silver_snapshot_version"]:
        raise ValueError("Wrong analyses or different Silver snapshots")
    expected = {
        "resultados_mensais.csv": finance["monthly"],
        "media_totais_mensais.csv": [{**finance["monthly_total_mean"], **finance["period"]}],
        "sensibilidade_financeira.csv": finance["negative_amount_sensitivity"],
        "passageiros_por_hora_maio.csv": people["hourly"],
        "sensibilidade_passageiros_maio.csv": people["sensitivity"],
        "resumo_passageiros_maio.csv": [people["period"]],
    }
    staging = ROOT / ".tmp" / ("export-" + uuid4().hex)
    staging.mkdir(parents=True)
    seen = set()
    for report in (finance, people):
        for source in report["csv_exports"].values():
            name = Path(source).name
            if not source.startswith("/Volumes/workspace/ifood_gold/results/") or name not in expected or name in seen:
                raise ValueError("Unexpected or isolated export path")
            subprocess.run(["databricks", "fs", "cp", "dbfs:" + source, str(staging / name), "--profile", "ifood-case"], check=True, capture_output=True)
            rows = list(csv.DictReader((staging / name).open(encoding="utf-8")))
            if len(rows) != len(expected[name]):
                raise ValueError("CSV row count differs from evidence: " + name)
            for row, reference in zip(rows, expected[name]):
                if set(row) != set(reference) or not all(equal(row[k], reference[k]) for k in row):
                    raise ValueError("CSV values differ from evidence: " + name)
            seen.add(name)
    if seen != set(expected):
        raise ValueError("Incomplete export")
    for name in seen:
        shutil.copyfile(staging / name, ROOT / "analysis" / name)
    print(json.dumps({"status":"VERIFIED", "csvs":len(seen), "silver_snapshot_version":finance["silver_snapshot_version"]}))

if __name__ == "__main__":
    main()
