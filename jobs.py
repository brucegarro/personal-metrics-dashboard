from datetime import date
import asyncio
from queueing import get_queue
from typing import Literal, TypedDict
from etl_metrics import etl_daily_sleep_day, etl_daily_readiness_day, etl_daily_atracker_task_entries

Endpoint = Literal["daily_sleep", "daily_readiness", "atracker"]

class EtlResult(TypedDict):
    endpoint: str
    date: str
    user_id: str
    inserted: int

def run_etl_job(endpoint: Endpoint, date_str: str, user_id: str) -> EtlResult:
    if endpoint == "daily_sleep":
        n = etl_daily_sleep_day(date_str, user_id)
    elif endpoint == "daily_readiness":
        n = etl_daily_readiness_day(date_str, user_id)
    elif endpoint == "atracker":
        # etl_daily_atracker_task_entries is async; run it in a fresh event loop
        n = asyncio.run(etl_daily_atracker_task_entries(user_id))
    else:
        raise ValueError(f"Unsupported endpoint: {endpoint}")
    return {"endpoint": endpoint, "date": date_str, "user_id": user_id, "inserted": n}

def enqueue_atracker_job(enqueued_jobs, user_id):
    q = get_queue("etl")
    job = q.enqueue(run_etl_job, "atracker", date.today().isoformat(), user_id)
    enqueued_jobs["atracker"] = job.id
