import time
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from metrics.oura_metrics import OuraMetrics, get_access_token_from_cache

USERID = "brucegarro"


app = FastAPI(title="Personal Metrics Dashboard")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/health")
async def health_check():
    oura_metrics = OuraMetrics()
    access_token = await get_access_token_from_cache(USERID)
    is_token_invalid = (access_token is None) or ( int(time.time()) > access_token.get("expires_at", 0) )

    if is_token_invalid:
        url = oura_metrics.get_oura_auth_url()
        return {
            "url": url,
            "status": "healthy"
        }
    
    oura_metrics.pull_data(access_token["access_token"])

    return {
        "access_token": access_token,
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
