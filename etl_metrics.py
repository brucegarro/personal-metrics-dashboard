import os
import json
from datetime import datetime
import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from db import SessionLocal, ENGINE
from models import Metric

BUCKET = os.getenv("S3_BUCKET")
ENV = os.getenv("S3_ENV", "dev")
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
REGION = os.getenv("S3_REGION", "us-east-1")
ACCESS = os.getenv("S3_ACCESS_KEY")
SECRET = os.getenv("S3_SECRET_KEY")
DEFAULT_USER_ID = os.getenv("DEFAULT_USER_ID", "user")


def _raw_glob(vendor: str, api: str, endpoint: str, date: str) -> str:
    # matches all hour partitions for that date
    return (
        f"s3://{BUCKET}/thirdparty/{vendor}/{api}/{ENV}/"
        f"zone=raw/endpoint={endpoint}/schema=v1/dt={date}/hour=*/part=*.jsonl.gz"
    )

def _s3_opts_json() -> str:
    opts = {
        "region": REGION,
        "aws_access_key_id": ACCESS,
        "aws_secret_access_key": SECRET,
    }
    if ENDPOINT_URL:
        opts["endpoint_url"] = ENDPOINT_URL
        # For MinIO in dev (http + path-style)
        if ENDPOINT_URL.startswith("http://"):
            opts["allow_http"] = "true"
        opts["virtual_hosted_style"] = "false"
    return opts

def _ensure_date(df: pl.DataFrame, col: str = "day") -> pl.DataFrame:
    if col not in df.columns:
        return df
    if df.schema.get(col) != pl.Date:
        return df.with_columns(pl.col(col).str.strptime(pl.Date, strict=False))
    return df

def _etl_daily_oura_day(
    endpoint: str,
    date_str: str,
    user_id: str,
    select_exprs: list[pl.Expr],
    vendor: str = "oura",
    api: str = "v2",
) -> int:
    scan = pl.scan_ndjson(
        _raw_glob(vendor, api, endpoint, date_str),
        storage_options=_s3_opts_json(),
    )
    df = scan.select("day", *select_exprs).collect(streaming=True)
    if df.height == 0:
        return 0

    df = _ensure_date(df, "day")
    metric_cols = [c for c in df.columns if c != "day"]

    long_df = (
        df.unpivot(index="day", on=metric_cols, variable_name="name", value_name="value")
          .drop_nulls("value")
          .with_columns(pl.col("value").cast(pl.Float64))
          .sort(["day", "name"])
    )

    payload = [
        {
            "name": r["name"],
            "user_id": user_id,
            "date": r["day"].item(),
            "endpoint": endpoint,
            "value": float(r["value"]),
        }
        for r in long_df.iter_rows(named=True)
    ]
    return _insert_metrics_ignore_conflicts(payload)


def etl_daily_sleep_day(date_str: str, user_id: str) -> int:
    return _etl_daily_oura_day(
        endpoint="daily_sleep",
        date_str=date_str,
        user_id=user_id,
        select_exprs=[
            pl.col("score").alias("sleep_score"),
            pl.col("contributors").struct.field("deep_sleep").alias("deep_sleep"),
            pl.col("contributors").struct.field("efficiency").alias("efficiency"),
            pl.col("contributors").struct.field("latency").alias("latency"),
            pl.col("contributors").struct.field("rem_sleep").alias("rem_sleep"),
            pl.col("contributors").struct.field("restfulness").alias("restfulness"),
            pl.col("contributors").struct.field("timing").alias("timing"),
            pl.col("contributors").struct.field("total_sleep").alias("total_sleep"),
        ],
    )

def etl_daily_readiness_day(date_str: str, user_id: str) -> int:
    return _etl_daily_oura_day(
        endpoint="daily_readiness",
        date_str=date_str,
        user_id=user_id,
        select_exprs=[
            pl.col("score").alias("readiness_score"),
        ],
    )


def _insert_metrics_ignore_conflicts(records: list[dict]) -> int:
    if not records:
        return 0
    stmt = (
        # pg_insert(Metric)
        insert(Metric)
        .values(records)
        .on_conflict_do_nothing(index_elements=["user_id", "date", "endpoint", "name"])
    )
    with SessionLocal() as s:
        res = s.execute(stmt)
        s.commit()
        return res.rowcount or 0


