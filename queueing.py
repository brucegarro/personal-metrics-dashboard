import os
from redis import Redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

def get_queue(name: str = "etl") -> Queue:
    conn = Redis.from_url(REDIS_URL)
    return Queue(name, connection=conn)