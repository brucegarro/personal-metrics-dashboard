import time
import logging
import sys

# Ensure logs are visible in Docker log feed
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout
)
from typing import Optional
from datetime import date, timedelta

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.responses import RedirectResponse

from metrics.oura.ingest import (
    get_oura_auth_url,
    get_and_cache_access_token,
    get_valid_access_token,
    pull_data,
)
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
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/dashboard")
def serve_dashboard():
    return FileResponse("static/dashboard.html")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/status")
async def status_check():
    return {"status": "ok"}


@app.get("/health")
async def health_check(redis_client=Depends(get_redis_client)):
    logger = logging.getLogger("health_check")
    access_token = await get_valid_access_token(USERID, redis_client=redis_client)
    logger.info(f"Fetched Oura access token for user {USERID} (valid or refreshed): {bool(access_token)}")

    oura_auth_url = None
    dropbox_auth_url = None
    oura_auth_valid = False
    dropbox_auth_valid = False

    if access_token is None:
        logger.info("Oura access token missing and cannot be refreshed; generating auth URL.")
        oura_auth_url = get_oura_auth_url()
    else:
        oura_auth_valid = True

    dbx_mgr = DropboxAuthManager()
    dbx_token = await get_dropbox_token(USERID)
    logger.info(f"Fetched Dropbox token for user {USERID}: {bool(dbx_token)}")
    dbx_token_expired = False
    if dbx_token and "expires_at" in dbx_token:
        dbx_token_expired = int(time.time()) > dbx_token["expires_at"]
    if not dbx_token or dbx_token_expired:
        logger.info("Dropbox token missing or expired, generating auth URL.")
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
            dropbox_auth_url = next_url
        else:
            dropbox_auth_url = dbx_mgr.get_authorize_url()
        dropbox_auth_valid = False
    else:
        dropbox_auth_valid = True

    # Get data from Oura
    default_start_date = date.today() - timedelta(days=90)
    default_end_date = date.today()

    if oura_auth_valid:
        job_id = pull_data(
            access_token["access_token"],
            start_date=default_start_date,
            end_date=default_end_date,
        )
        logger.info(f"Oura ETL job enqueued: {job_id}")

    # Enqueue Atracker ETL job at most once every 2 minutes using a Redis lock
    enqueued_jobs = {}
    try:
        lock_key = "locks:atracker:enqueue"
        can_enqueue = await redis_client.set(lock_key, "1", ex=60, nx=True)
    except Exception:
        # If redis doesn't support SET NX in this context (e.g., tests), allow enqueue
        can_enqueue = True
    if can_enqueue:
        enqueue_atracker_job(enqueued_jobs, USERID)
        logger.info(f"Atracker ETL job enqueued: {enqueued_jobs.get('atracker')}")
    else:
        logger.info("Atracker ETL enqueue skipped due to lock (within 2 minutes).")

     # Always return metrics_view, even if Oura/Dropbox auth is not valid
    metrics_view = get_metrics_pivot(
        USERID,
        default_start_date,
        default_end_date,
    )

    return {
        "metrics_view": metrics_view,
        "oura_auth_url": oura_auth_url,
        "oura_auth_valid": oura_auth_valid,
        "dropbox_auth_url": dropbox_auth_url,
        "dropbox_auth_valid": dropbox_auth_valid,
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
    return RedirectResponse(url="/dashboard")

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

    return RedirectResponse(url="/dashboard")
