import os
import json
import time
import requests
from datetime import date, timedelta
from typing import Any, Dict, Optional 

from oura import OuraOAuth2Client
from redis.asyncio import Redis

from s3io import write_jsonl_gz


OURA_CLIENT_ID = os.environ["OURA_CLIENT_ID"]
OURA_CLIENT_SECRET = os.environ["OURA_CLIENT_SECRET"]
OURA_REDIRECT_URI=os.environ["OURA_REDIRECT_URI"]

REDIS_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# _redis = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
_redis = Redis(host="redis", port=6379, encoding="utf-8", decode_responses=True)

def _key(user_id: str) -> str:
    return f"auth:token:{user_id}"

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
    
    def get_data_from_api(self, access_token: str, endpoint: str) -> Dict[str, Any]:
        """
        endpoint: can be one of: 'daily_sleep', 'daily_readiness", "daily_activity", etc.
        """
        url = 'https://api.ouraring.com/v2/usercollection/%s' % endpoint
        headers = { 
            'Authorization': 'Bearer %s' % access_token
        }

        params = {
            'start_date': (date.today() - timedelta(days=90)).isoformat(),
            'end_date': (date.today() - timedelta(days=0)).isoformat()
        }
        response = requests.request('GET', url, headers=headers, params=params)
        return response.json()["data"]
    
    def pull_data(self, access_token: str) -> Dict[str, Any]:
        data = {}
        endpoints = [
            'daily_sleep',
            'daily_readiness',
            # 'daily_activity'
        ]

        # Get data from Oura
        for endpoint in endpoints:
            data[endpoint] = self.get_data_from_api(access_token, endpoint)

        # Write raw data to S3 buckets
        for endpoint in endpoints:
            write_jsonl_gz(
                records=data.get("daily_sleep", []),
                vendor="oura",
                api="v2",
                endpoint=endpoint,
                schema="v1"
            )

        # TODO: Format data

        # Recommended: Use PostgreSQL with SQLAlchemy ORM for FastAPI
        # Example:
        # from sqlalchemy.orm import Session
        # from . import models, schemas
        # db: Session = get_db()
        # db_data = models.SleepData(**formatted_data)
        # db.add(db_data)
        # db.commit()

        # TODO: store data in a database

        return data

        # TODO: Format data
        
        # TODO: store data in a database


        return data