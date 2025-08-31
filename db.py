import os
from contextlib import contextmanager
from datetime import date as Date
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker

from models import SeenEvent

ENGINE = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

@contextmanager
def _conn():
    with ENGINE.begin() as conn:
        yield conn

def mark_event_seen(user_id: str, date: Date, endpoint: str) -> None:
    pass

def mark_event_seen_bulk(rows: list[tuple[str, Date, str]]) -> None:
    pass


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