"""Nomes compartilhados dos objetos do case principal e das reproduções isoladas."""
from dataclasses import dataclass
from pathlib import Path
import re

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAYERS = ("bronze", "silver", "gold", "ops")
MONTHS = tuple(f"2023-{month:02d}" for month in range(1, 6))


@dataclass(frozen=True)
class CaseConfig:
    catalog: str = "workspace"
    schema_prefix: str = "ifood"
    landing_catalog: str = "workspace"
    landing_schema: str = "ifood_landing"

    def __post_init__(self):
        for name, value in vars(self).items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                raise ValueError(f"Invalid SQL identifier: {name}")
        if not re.fullmatch(r"ifood(?:_[a-z0-9_]+)?", self.schema_prefix):
            raise ValueError("Schema prefix must belong to the ifood case")

    def schema(self, layer):
        if layer not in LAYERS:
            raise ValueError("Unknown layer")
        return f"{self.catalog}.{self.schema_prefix}_{layer}"

    def table(self, layer, name):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Invalid object identifier")
        return f"{self.schema(layer)}.{name}"

    @property
    def landing_root(self):
        return Path(f"/Volumes/{self.landing_catalog}/{self.landing_schema}/raw")

    @property
    def results_root(self):
        return Path(f"/Volumes/{self.catalog}/{self.schema_prefix}_gold/results")

    @property
    def project_root(self):
        return PROJECT_ROOT

    def bind_sql(self, sql):
        return sql.replace("workspace.ifood_gold.yellow_trips", self.table("gold", "yellow_trips"))


def load_case_config(dbutils):
    defaults = {"catalog": "workspace", "schema_prefix": "ifood",
                "landing_catalog": "workspace", "landing_schema": "ifood_landing"}
    for name, value in defaults.items():
        dbutils.widgets.text(name, value)
    return CaseConfig(**{name: dbutils.widgets.get(name) for name in defaults})
