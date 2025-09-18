from datetime import date
import asyncio
from queueing import get_queue
from typing import Literal, TypedDict

Endpoint = Literal["daily_sleep", "daily_readiness", "atracker", "atracker_file"]

class EtlResult(TypedDict):
    endpoint: str
    date: str
    user_id: str
    inserted: int

def run_etl_job(endpoint: Endpoint, date_str: str, user_id: str) -> EtlResult:
    # Lazy-import to keep app process memory light; heavy deps loaded only in worker.
    if endpoint == "daily_sleep":
        from etl_metrics import etl_daily_sleep_day
        n = etl_daily_sleep_day(date_str, user_id)
    elif endpoint == "daily_readiness":
        from etl_metrics import etl_daily_readiness_day
        n = etl_daily_readiness_day(date_str, user_id)
    elif endpoint == "atracker":
        # Orchestrator: sync folder and enqueue per-file jobs
        from etl_metrics import etl_daily_atracker_task_entries
        n = asyncio.run(etl_daily_atracker_task_entries(user_id))
    elif endpoint == "atracker_file":
        from etl_metrics import atracker_process_file
        # date_str carries the file path for per-file processing
        n = atracker_process_file(date_str, user_id)
    else:
        raise ValueError(f"Unsupported endpoint: {endpoint}")
    return {"endpoint": endpoint, "date": date_str, "user_id": user_id, "inserted": n}

def enqueue_atracker_job(enqueued_jobs, user_id):
    q = get_queue("etl")
    job = q.enqueue(run_etl_job, "atracker", date.today().isoformat(), user_id)
    enqueued_jobs["atracker"] = job.id
