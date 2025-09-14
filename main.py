import time
from typing import Optional
from datetime import date, timedelta

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from metrics.oura.ingest import get_oura_auth_url, get_and_cache_access_token, get_access_token_from_cache, pull_data
from metrics.view import get_metrics_pivot
from metrics.atracker.dropbox import DropboxAuthManager, get_dropbox_token
import os
from queueing import get_queue
from jobs import run_etl_job, enqueue_atracker_job

USERID = "brucegarro"
DROPBOX_REDIRECT_URI = os.getenv("DROPBOX_REDIRECT_URI")
DOMAIN = os.getenv("DOMAIN")



from fastapi import Depends

def get_redis_client():
    from metrics.oura.ingest import _redis
    return _redis

app = FastAPI(title="Personal Metrics Dashboard")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/health")
async def health_check(redis_client=Depends(get_redis_client)):
    access_token = await get_access_token_from_cache(USERID, redis_client=redis_client)
    is_expired = access_token and int(time.time()) > access_token.get("expires_at", 0)

    if access_token is None or is_expired:
        url = get_oura_auth_url()
        return {
            "url": url,
            "status": "healthy"
        }

    # Ensure Dropbox auth exists before Atracker ETL
    dbx_mgr = DropboxAuthManager()
    dbx_token = await get_dropbox_token(USERID)
    if not dbx_token:
        if DROPBOX_REDIRECT_URI:
            next_url = "/dropbox_start"
            if DOMAIN:
                host = DOMAIN
                if host.startswith("http://"):
                    host = host[len("http://"):]
                if host.startswith("https://"):
                    host = host[len("https://"):]
                host = host.strip("/")
                next_url = f"https://{DOMAIN}/dropbox_start"
            return {
                "dropbox_auth_required": True,
                "next": next_url,
                "status": "healthy",
            }
        else:
            dropbox_url = dbx_mgr.get_authorize_url()
            return {
                "dropbox_url": dropbox_url,
                "message": "Authorize Dropbox and then call /dropbox_finish?code=...",
                "status": "healthy"
            }

    # Get data from Oura
    default_start_date = date.today() - timedelta(days=90)
    default_end_date = date.today()

    api_data, persisted_data, enqueued_jobs = pull_data(
        access_token["access_token"],
        start_date=default_start_date,
        end_date=default_end_date,
    )

    enqueue_atracker_job(enqueued_jobs, USERID)

    metrics_view = get_metrics_pivot(
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

@app.get("/dropbox_finish")
async def dropbox_finish(code: str):
    mgr = DropboxAuthManager()
    await mgr.finish_no_redirect(USERID, code)
    return RedirectResponse(url="/health")

@app.get("/dropbox_start")
async def dropbox_start():
    if not DROPBOX_REDIRECT_URI:
        return {"error": "DROPBOX_REDIRECT_URI is not configured"}
    mgr = DropboxAuthManager()
    url = await mgr.get_authorize_url_redirect(USERID, DROPBOX_REDIRECT_URI)
    return RedirectResponse(url=url)

@app.get("/dropbox_callback")
async def dropbox_callback(code: str, state: str):
    if not DROPBOX_REDIRECT_URI:
        return {"error": "DROPBOX_REDIRECT_URI is not configured"}
    mgr = DropboxAuthManager()
    await mgr.finish_redirect(USERID, code, state, DROPBOX_REDIRECT_URI)
    return RedirectResponse(url="/health")

@app.get("/oura_callback")
async def handle_callback(
    code: str,
    state: str,
    error: Optional[str] = None,
    redis_client=Depends(get_redis_client)
):
    """
    Handles a GET request to the /callback endpoint,
    retrieving 'code', 'state', and 'error' query parameters.
    """
    if error:
        return {"message": f"Error during callback: {error}"}

    access_token = await get_and_cache_access_token(code, redis_client=redis_client)

    return RedirectResponse(url="/health")
