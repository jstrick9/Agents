"""Shared catalog/schema/volume resolution for notebooks and jobs.

Widgets (dbutils) win, then spark.conf, then defaults.
Commercial POC host is https://dbc-ae83c2ba-d87c.cloud.databricks.com
(GovCloud/DoD remains the proposed production boundary).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "default"
DEFAULT_AWS_REGION = "us-east-2"
POC_WORKSPACE_HOST = "https://dbc-ae83c2ba-d87c.cloud.databricks.com"
POC_WORKSPACE_ORG_ID = "7474653232339519"
POC_WORKSPACE_FOLDER = "/Workspace/Users/joshua.strickland@satsyil.com/onr_itss_poc"


@dataclass(frozen=True)
class OnrConfig:
    catalog: str
    schema: str
    aws_region: str = DEFAULT_AWS_REGION
    environment: str = "dev"

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def landing_volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/landing"

    @property
    def export_volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/export"

    @property
    def checkpoint_volume(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/checkpoints"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def landing(self, *parts: str) -> str:
        suffix = "/".join(parts)
        return f"{self.landing_volume}/{suffix}" if suffix else self.landing_volume


def _widget(dbutils: Any, name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name)
        return value or default
    except Exception:
        return default


def _conf(spark: Any, key: str, default: str) -> str:
    if spark is None:
        return default
    try:
        return spark.conf.get(key, default)
    except Exception:
        try:
            return spark.conf.get(key)
        except Exception:
            return default


def load_config(
    dbutils: Any = None,
    spark: Any = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> OnrConfig:
    cat = catalog or ( _widget(dbutils, "catalog", DEFAULT_CATALOG) if dbutils else None )
    sch = schema or ( _widget(dbutils, "schema", DEFAULT_SCHEMA) if dbutils else None )
    cat = cat or _conf(spark, "onr.catalog", DEFAULT_CATALOG)
    sch = sch or _conf(spark, "onr.schema", DEFAULT_SCHEMA)
    env = _conf(spark, "onr.environment", "dev")
    region = _conf(spark, "onr.aws_region", DEFAULT_AWS_REGION)
    return OnrConfig(catalog=cat, schema=sch, aws_region=region, environment=env)
