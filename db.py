import os
from contextlib import contextmanager
from datetime import date as Date
from typing import Sequence, Set
from sqlalchemy import create_engine, text, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from models import SeenEvent

ENGINE = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

@contextmanager
def _conn():
    with ENGINE.begin() as conn:
        yield conn


def get_seen_events(user_id: str, endpoint: str, start_date: Date, end_date: Date) -> list[SeenEvent]:
    with SessionLocal() as s:
        stmt = (
            select(SeenEvent)
            .where(
                SeenEvent.user_id == user_id,
                SeenEvent.endpoint == endpoint,
                SeenEvent.date >= start_date,
                SeenEvent.date <= end_date,
            )
        )
        return s.scalars(stmt).all()


def create_seen_events_bulk(
    user_id: str,
    endpoint: str,
    dates: Set[Date],
) -> Set[Date]:
    """
    Bulk-insert seen_events rows for (user_id, endpoint) on the given dates.
    Uses ON CONFLICT DO NOTHING against the composite PK (user_id, date, endpoint).
    Returns the set of dates that were newly inserted.
    """
    if not dates:
        return set()

    # de-dup input to avoid unnecessary conflicts
    unique_dates = sorted(set(dates))

    payload = [
        {"user_id": user_id, "endpoint": endpoint, "date": d}
        for d in unique_dates
    ]

    stmt = (
        insert(SeenEvent)
        .values(payload)
        .on_conflict_do_nothing(index_elements=["user_id", "date", "endpoint"])
        .returning(SeenEvent.date)  # returns only rows actually inserted
    )

    with SessionLocal() as s:
        inserted_dates = {row[0] for row in s.execute(stmt)}
        s.commit()

    return inserted_dates