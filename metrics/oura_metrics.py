import os
import json
import time
import requests
from datetime import date, datetime, timedelta
from typing import Any, Dict, Tuple, Optional

from oura import OuraOAuth2Client
from s3io import write_jsonl_gz

from db import get_seen_events, create_seen_events_bulk, get_metrics
from queueing import get_queue
from jobs import run_etl_job
from auth.cache import get_async_redis, REDIS_TTL_SECONDS, auth_key


OURA_CLIENT_ID = os.environ["OURA_CLIENT_ID"]
OURA_CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
OURA_REDIRECT_URI=os.environ["OURA_REDIRECT_URI"]

_redis = get_async_redis()

def _key(user_id: str) -> str:
    return auth_key("oura", user_id)

async def cache_access_token_from_cache(user_id: str, token: Dict[str, Any], ttl: int = REDIS_TTL_SECONDS) -> None:
    """
    token should include at least: access_token, expires_at (epoch seconds)
    """
    if "expires_at" not in token:
        token["expires_at"] = int(time.time()) + token.get("expires_in", REDIS_TTL_SECONDS)
    # you can encrypt token here if desired (see notes below)
    await _redis.set(_key(user_id), json.dumps(token), ex=ttl)

async def get_access_token_from_cache(user_id: str) -> Optional[Dict[str, Any]]:
    raw = await _redis.get(_key(user_id))
    return json.loads(raw) if raw else None

async def delete_access_token(user_id: str) -> None:
    await _redis.delete(_key(user_id))


class OuraMetrics:
    def __init__(self):
        self.auth_client = OuraOAuth2Client(client_id=OURA_CLIENT_ID, client_secret=OURA_CLIENT_SECRET)
        self.auth_client.session.scope = "All scopes"
        self.auth_client.session.redirect_uri = OURA_REDIRECT_URI
        self.client = None

    def get_oura_auth_url(self):
        url, state = self.auth_client.authorize_endpoint()
        return url

    async def get_and_cache_access_token(self, code: str):
        token_dict = self.auth_client.fetch_access_token(code=code)
        access_token = token_dict.get("access_token")

        user_key = "brucegarro"  # Replace with actual user ID or username
        await cache_access_token_from_cache(user_key, token_dict)

        return access_token
    
    def get_data_from_api(self, access_token: str, endpoint: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        endpoint: can be one of: 'daily_sleep', 'daily_readiness", "daily_activity", etc.
        """
        url = 'https://api.ouraring.com/v2/usercollection/%s' % endpoint
        headers = { 
            'Authorization': 'Bearer %s' % access_token
        }

        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
        }
        response = requests.request('GET', url, headers=headers, params=params)
        return response.json()["data"]
    
    def get_metrics_pivot(self, user_id: str, start_date: date, end_date: date) -> list[dict]:
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

    def pull_data(self, access_token: str, start_date: date, end_date: date, user_id="brucegarro") -> Tuple[Dict[str, Any], Dict[str, Any]]:
        api_data = {}
        endpoints = [
            'daily_sleep',
            'daily_readiness',
        ]
        
        persisted_data = {}
        enqueued_jobs = {}
        for endpoint in endpoints:
            seen_events = { event.date for event in get_seen_events(
                user_id=user_id,
                endpoint=endpoint,
                start_date=start_date,
                end_date=end_date,
            ) }
            unseen_dates = {
                start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
            } - seen_events


            if unseen_dates:
                api_data[endpoint] = [
                    r for r in
                    self.get_data_from_api(
                        access_token,
                        endpoint,
                        min(unseen_dates),
                        max(unseen_dates)
                    ) if datetime.fromisoformat(r["timestamp"]).date() in unseen_dates
                ]

            if api_data[endpoint]:
                # Write raw data to S3 buckets
                persisted_data[endpoint] = write_jsonl_gz(
                    records=api_data.get(endpoint, []),
                    vendor="oura",
                    api="v2",
                    endpoint=endpoint,
                    schema="v1"
                )

                # Mark events as seen in the DB
                create_seen_events_bulk(
                    user_id=user_id,
                    endpoint=endpoint,
                    dates={
                        datetime.fromisoformat(item["timestamp"]).date()
                        for item in api_data.get(endpoint, [])
                    }
                )

                # Enqueue ETL job for storing to the DB
                q = get_queue("etl")
                job = q.enqueue(run_etl_job, endpoint, date.today().isoformat(), user_id)
                enqueued_jobs[endpoint] = job.id

        return api_data, persisted_data, enqueued_jobs
