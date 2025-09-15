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

async def cache_access_token_from_cache(user_id: str, token: Dict[str, Any], ttl: int = REDIS_TTL_SECONDS, redis_client=None) -> None:
	"""
	token should include at least: access_token, expires_at (epoch seconds)
	"""
	if redis_client is None:
		redis_client = _redis
	if "expires_at" not in token:
		token["expires_at"] = int(time.time()) + token.get("expires_in", REDIS_TTL_SECONDS)
	await redis_client.set(_key(user_id), json.dumps(token), ex=ttl)

async def get_access_token_from_cache(user_id: str, redis_client=None) -> Optional[Dict[str, Any]]:
	if redis_client is None:
		redis_client = _redis
	raw = await redis_client.get(_key(user_id))
	return json.loads(raw) if raw else None

async def delete_access_token(user_id: str, redis_client=None) -> None:
	if redis_client is None:
		redis_client = _redis
	await redis_client.delete(_key(user_id))

def get_oura_auth_url():
	client = OuraOAuth2Client(client_id=OURA_CLIENT_ID, client_secret=OURA_CLIENT_SECRET)
	client.session.scope = "All scopes"
	client.session.redirect_uri = OURA_REDIRECT_URI
	url, state = client.authorize_endpoint()
	return url

async def get_and_cache_access_token(code: str, user_key: str = "brucegarro", redis_client=None):
	client = OuraOAuth2Client(client_id=OURA_CLIENT_ID, client_secret=OURA_CLIENT_SECRET)
	client.session.scope = "All scopes"
	client.session.redirect_uri = OURA_REDIRECT_URI
	token_dict = client.fetch_access_token(code=code)
	access_token = token_dict.get("access_token")
	await cache_access_token_from_cache(user_key, token_dict, redis_client=redis_client)
	return access_token

def get_data_from_api(access_token: str, endpoint: str, start_date: date, end_date: date) -> Dict[str, Any]:
	url = f'https://api.ouraring.com/v2/usercollection/{endpoint}'
	headers = { 'Authorization': f'Bearer {access_token}' }
	params = {
		'start_date': start_date.isoformat(),
		'end_date': end_date.isoformat(),
	}
	response = requests.request('GET', url, headers=headers, params=params)
	return response.json()["data"]

def pull_data(access_token: str, start_date: date, end_date: date, user_id="brucegarro") -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
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
				get_data_from_api(
					access_token,
					endpoint,
					min(unseen_dates),
					max(unseen_dates)
				) if datetime.fromisoformat(r["timestamp"]).date() in unseen_dates
			]

		if api_data.get(endpoint):
			persisted_data[endpoint] = write_jsonl_gz(
				records=api_data.get(endpoint, []),
				vendor="oura",
				api="v2",
				endpoint=endpoint,
				schema="v1"
			)

			create_seen_events_bulk(
				user_id=user_id,
				endpoint=endpoint,
				dates={
					datetime.fromisoformat(item["timestamp"]).date()
					for item in api_data.get(endpoint, [])
				}
			)

			q = get_queue("etl")
			job = q.enqueue(run_etl_job, endpoint, date.today().isoformat(), user_id, job_timeout=300)
			enqueued_jobs[endpoint] = job.id

	return api_data, persisted_data, enqueued_jobs
