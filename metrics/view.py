# Functions for restructuring metrics data for display in the app
from datetime import datetime
from db import get_metrics

def get_metrics_pivot(user_id: str, start_date, end_date) -> list[dict]:
    metrics = get_metrics(user_id, start_date, end_date)
    pivoted = {}
    for metric in metrics:
        day_str = metric.date.isoformat()
        if day_str not in pivoted:
            pivoted[day_str] = {"date": day_str}
        pivoted[day_str][metric.name] = metric.value
    pivoted_list = list(pivoted.values())
    pivoted_list.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
    return pivoted_list
