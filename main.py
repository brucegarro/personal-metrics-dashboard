import time
from typing import Optional
from datetime import date, timedelta

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from metrics.oura_metrics import OuraMetrics, get_access_token_from_cache
from queueing import get_queue
from jobs import run_etl_job

USERID = "brucegarro"


app = FastAPI(title="Personal Metrics Dashboard")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/health")
async def health_check():
    oura_metrics = OuraMetrics()
    access_token = await get_access_token_from_cache(USERID)
    is_expired = access_token and int(time.time()) > access_token.get("expires_at", 0)

    if access_token is None or is_expired:
        url = oura_metrics.get_oura_auth_url()
        return {
            "url": url,
            "status": "healthy"
        }
    
    # Get data from Oura
    default_start_date = date.today() - timedelta(days=90)
    default_end_date = date.today()

    api_data, persisted_data, enqueued_jobs = oura_metrics.pull_data(
        access_token["access_token"],
        start_date=default_start_date,
        end_date=default_end_date,
    )

    q = get_queue("etl")
    job = q.enqueue(run_etl_job, "atracker", date.today().isoformat(), USERID)
    enqueued_jobs["atracker"] = job.id

    metrics_view = oura_metrics.get_metrics_pivot(
        USERID,
        default_start_date,
        default_end_date,
    )

    return {
        "metrics_view": metrics_view,
        "api_data": api_data,
        "persisted_data": persisted_data,
        "enqueued_jobs": enqueued_jobs,
        "status": "healthy"
    }

@app.get("/oura_callback")
async def handle_callback(
    code: str,
    state: str,
    error: Optional[str] = None
):
    """
    Handles a GET request to the /callback endpoint,
    retrieving 'code', 'state', and 'error' query parameters.
    """
    if error:
        return {"message": f"Error during callback: {error}"}
    
    oura_metrics = OuraMetrics()
    access_token = await oura_metrics.get_and_cache_access_token(code)
    
    # return {
    #     "access_token": access_token,
    #     "state": state,
    #     "message": "Callback handled successfully"
    # }

    return RedirectResponse(url="/health")
