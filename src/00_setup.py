# Databricks notebook source
"""Cria schemas e volume gerenciado de landing sem modificar tabelas existentes."""
import json
import sys
sys.path.insert(0, "/Workspace/Shared/ifood-case-data/src")
from case_config import load_case_config, LAYERS

config = load_case_config(dbutils)
for layer in LAYERS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema(layer)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.landing_catalog}.{config.landing_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.landing_catalog}.{config.landing_schema}.raw")
dbutils.notebook.exit(json.dumps({"status":"READY","schemas":[config.schema(layer) for layer in LAYERS],
                                "landing":str(config.landing_root),"existing_tables_overwritten":False}))
