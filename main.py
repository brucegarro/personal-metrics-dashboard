from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Personal Metrics Dashboard")

class Metric(BaseModel):
    name: str
    value: float
    unit: str

@app.get("/")
def read_root():
    return {"message": "Welcome to the Personal Metrics Dashboard"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

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
