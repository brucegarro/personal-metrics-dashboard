from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from metrics.oura import OuraMetrics


app = FastAPI(title="Personal Metrics Dashboard")

class Metric(BaseModel):
    name: str
    value: float
    unit: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/oura_callback")
async def handle_callback(
    code: str,
    state: Optional[str] = None,
    error: Optional[str] = None
):
    """
    Handles a GET request to the /callback endpoint,
    retrieving 'code', 'state', and 'error' query parameters.
    """
    if error:
        return {"message": f"Error during callback: {error}"}
    
    oura_metrics = OuraMetrics()
    oura_metrics.handle_callback(code)
    
    return {
        "code": code,
        "state": state,
        "message": "Callback handled successfully"
    }


@app.get("/health")
def health_check():
    return {
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

@app.post("/metric/")
def create_metric(metric: Metric):
    # Placeholder for creating a new metric
    return {
        "message": "Metric created successfully",
        "metric": metric
    }
