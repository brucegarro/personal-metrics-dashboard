from typing import Optional

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from metrics.oura_metrics import OuraMetrics, get_access_token


app = FastAPI(title="Personal Metrics Dashboard")
USERID = "brucegarro"


@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

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
    access_token = await oura_metrics.handle_callback(code)
    
    # return {
    #     "access_token": access_token,
    #     "state": state,
    #     "message": "Callback handled successfully"
    # }

    return RedirectResponse(url="/health")


@app.get("/health")
async def health_check():
    oura_metrics = OuraMetrics()
    access_token = await get_access_token(USERID)
    if not access_token:
        url = oura_metrics.get_oura_auth_url()
        return {
            "url": url,
            "status": "healthy"
        }
    
    return {
        "access_token": access_token,
        "status": "healthy"
    }

@app.get("/metric/{metric_id}")
def get_metric(metric_id: int):
    # Placeholder for fetching a metric by ID
    return {
        "metric_id": metric_id,
        "name": "Sample Metric",
        "value": 42.0,
        "unit": "units"
    }
