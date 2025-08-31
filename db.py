import os
from contextlib import contextmanager
from datetime import date as Date
from sqlalchemy import create_engine, text

ENGINE = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)

@contextmanager
def _conn():
    with ENGINE.begin() as conn:
        yield conn

def mark_event_seen(user_id: str, date: Date, endpoint: str) -> None:
    """
    Insert one (user_id, date, endpoint). Idempotent via ON CONFLICT on the composite PK.
    """
    sql = text("""
        INSERT INTO seen_events (user_id, date, endpoint)
        VALUES (:user_id, :date, :endpoint)
        ON CONFLICT (user_id, date, endpoint) DO NOTHING
    """)
    with _conn() as c:
        c.execute(sql, {"user_id": user_id, "date": date, "endpoint": endpoint})

def mark_event_seen_bulk(rows: list[tuple[str, Date, str]]) -> None:
    """
    rows = [(user_id, date, endpoint), ...]
    """
    if not rows:
        return
    sql = text("""
        INSERT INTO seen_events (user_id, date, endpoint)
        VALUES (:user_id, :date, :endpoint)
        ON CONFLICT (user_id, date, endpoint) DO NOTHING
    """)
    payload = [{"user_id": u, "date": d, "endpoint": e} for (u, d, e) in rows]
    with _conn() as c:
        c.execute(sql, payload)

def get_seen_events(user_id: str, endpoint: str, start_date: Date, end_date: Date) -> bool:
    sql = text("""
        SELECT *
        FROM seen_events
        WHERE user_id = :user_id
          AND endpoint = :endpoint
          AND date BETWEEN :start_date AND :end_date
    """)
    with ENGINE.connect() as c:
        return c.execute(sql, {
            "user_id": user_id,
            "endpoint": endpoint,
            "start_date": start_date,
            "end_date": end_date
        })