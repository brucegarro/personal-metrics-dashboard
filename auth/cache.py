import os
from typing import Optional

from redis.asyncio import Redis


# Shared Redis settings for token caching
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", str(24 * 60 * 60)))

_redis_client: Optional[Redis] = None


def get_async_redis() -> Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    return _redis_client


def auth_key(provider: str, user_id: str) -> str:
    return f"auth:{provider}:token:{user_id}"

