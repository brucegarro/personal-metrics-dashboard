import logging
from enum import Enum
from datetime import datetime
from db import iter_metrics

class MetricCategory(Enum):
    WELLNESS = "wellness"
    PRODUCTIVITY = "productivity"

    @staticmethod
    def from_endpoint(endpoint: str):
        if endpoint in ["daily_sleep", "daily_readiness"]:
            return MetricCategory.WELLNESS
        elif endpoint == "atracker":
            return MetricCategory.PRODUCTIVITY
        else:
            raise ValueError(f"Unknown endpoint: {endpoint}")

def get_metrics_pivot(user_id: str, start_date, end_date) -> list[dict]:
    logger = logging.getLogger("metrics_view")
    logger.info(f"Fetching metrics for user {user_id} from {start_date} to {end_date}")
    count = 0
    pivoted = {}
    for metric in iter_metrics(user_id, start_date, end_date):
        count += 1
        day_str = metric.date.isoformat()
        if not day_str in pivoted:
            pivoted[day_str] = {
                category.value: {} for category in MetricCategory
            }
            pivoted[day_str]["date"] = day_str
        category = MetricCategory.from_endpoint(
            metric.endpoint
        ).value
        pivoted[day_str][category][metric.name] = metric.value
    pivoted_list = list(pivoted.values())
    logger.info(f"Fetched {count} metric rows and pivoted into {len(pivoted_list)} days.")
    pivoted_list.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))

    return pivoted_list
