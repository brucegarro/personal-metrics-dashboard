import os
import json
from datetime import datetime
from collections import defaultdict
from zoneinfo import ZoneInfo
import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from db import SessionLocal, ENGINE
from models import Metric
from s3io import _list_ndjson_gz_keys, _load_ndjson_gz_as_polars
from metrics.atracker.ingest import sync_folder, parse_atracker_datafile
from db import upsert_task_entries_row_by_row, task_entry_from_json, aggregate_task_entries_to_metrics

BUCKET = os.getenv("S3_BUCKET")
ENV = os.getenv("S3_ENV", "dev")
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
REGION = os.getenv("S3_REGION", "us-east-1")
ACCESS = os.getenv("S3_ACCESS_KEY")
SECRET = os.getenv("S3_SECRET_KEY")
DEFAULT_USER_ID = os.getenv("DEFAULT_USER_ID", "user")


### ATRACKER ETL
def etl_daily_atracker_task_entries(user_id: str) -> int:
    downloaded_files = sync_folder(user_id=user_id)
    updates = 0
    created = []; updated = []; unchanged = []

    # Create or update TaskEntry rows
    for filepath in downloaded_files:
        task_entries = [
            task_entry_from_json(task_entry_json)
            for task_entry_json
            in parse_atracker_datafile(filepath)
        ]
        _created, _updated, _unchanged = upsert_task_entries_row_by_row(task_entries)
        created += _created
        updated += _updated
        unchanged += _unchanged

    updates += len(created) + len(updated)
    dates = { entry.start_time.date() for entry in created + updated }

    if not dates:
        return updates

    metrics_created, metrics_updated = aggregate_task_entries_to_metrics(dates, user_id)
    updates += metrics_created + metrics_updated

    return updates

## OURA ETL
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
    col_map: dict[str, str] | None = None,
    struct_map: dict[str, str] | None = None,
    struct_col: str = "contributors",
    vendor: str = "oura",
    api: str = "v2",
) -> int:
    col_map = col_map or {}
    struct_map = struct_map or {}

    keys = _list_ndjson_gz_keys(vendor, api, endpoint, date_str)
    if not keys:
        return 0

    df = _load_ndjson_gz_as_polars(keys)
    if df.height == 0:
        return 0

    select_list = [pl.col("day")]
    for src, dst in col_map.items():
        select_list.append(pl.col(src).alias(dst))
    for sfield, dst in struct_map.items():
        select_list.append(pl.col(struct_col).struct.field(sfield).alias(dst))

    df = df.select(select_list)
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

    payload = []
    for r in long_df.iter_rows(named=True):
        payload.append({
            "name": r["name"],
            "user_id": user_id,
            "date": r["day"],
            "endpoint": endpoint,
            "value": float(r["value"]),
        })
    return _insert_metrics_ignore_conflicts(payload)


def etl_daily_sleep_day(date_str: str, user_id: str) -> int:
    return _etl_daily_oura_day(
        endpoint="daily_sleep",
        date_str=date_str,
        user_id=user_id,
        col_map={"score": "sleep_score"},
        struct_map={
            "deep_sleep": "deep_sleep",
            "efficiency": "efficiency",
            "latency": "latency",
            "rem_sleep": "rem_sleep",
            "restfulness": "restfulness",
            "timing": "timing",
            "total_sleep": "total_sleep",
        },
    )

def etl_daily_readiness_day(date_str: str, user_id: str) -> int:
    return _etl_daily_oura_day(
        endpoint="daily_readiness",
        date_str=date_str,
        user_id=user_id,
        col_map={"score": "readiness_score"},
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
