import os
import re
import unicodedata
from contextlib import contextmanager
from datetime import date as Date
from zoneinfo import ZoneInfo
from typing import Sequence, Set, List, Tuple
from collections import defaultdict
from sqlalchemy import create_engine, text, select, Date as SQLDate
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from models import Metric, SeenEvent, TaskEntry

ENGINE = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

EST = ZoneInfo("America/New_York")

def aggregate_task_entries_to_metrics(dates: set[Date], user_id: str) -> tuple[int, int]:
    """
    Aggregate TaskEntry rows into daily Metric rows.
    Skips deleted tasks and those without end_time.
    
    Returns: (metrics_created, metrics_updated)
    """
    created, updated = 0, 0
    task_hours: dict[tuple[str, Date], float] = defaultdict(float)

    with SessionLocal() as s:
        entries = (
            s.query(TaskEntry)
            .filter(TaskEntry.start_time.cast(SQLDate).in_(dates))
            .all()
        )

        for entry in entries:
            if not entry.end_time or entry.deleted_new:
                continue  # skip incomplete or deleted tasks

            local_date = entry.start_time.astimezone(EST).date()
            hours = (entry.end_time - entry.start_time).total_seconds() / 3600.0
            task_hours[(entry.task_id, local_date)] += hours

        for (task_id, date), total_hours in task_hours.items():
            metric = (
                s.query(Metric)
                .filter_by(
                    user_id=user_id,
                    date=date,
                    endpoint="atracker",
                    name=task_id,
                )
                .one_or_none()
            )
            if metric:
                metric.value = total_hours
                updated += 1
            else:
                metric = Metric(
                    name=task_id,
                    user_id=user_id,
                    date=date,
                    endpoint="atracker",
                    value=total_hours,
                )
                s.add(metric)
                created += 1

        s.commit()

    return created, updated

@contextmanager
def _conn():
    with ENGINE.begin() as conn:
        yield conn



def get_metrics(user_id: str, start_date: Date, end_date: Date) -> list[dict]:
    with SessionLocal() as s:
        stmt = (
            select(Metric.date, Metric.endpoint, Metric.name, Metric.value)
            .where(
                Metric.user_id == user_id,
                Metric.date >= start_date,
                Metric.date <= end_date,
            )
            .order_by(Metric.date, Metric.endpoint, Metric.name)
        )
        return s.execute(stmt).all()


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


from models import TaskEntry, ms_to_datetime

def clean_task_id(raw_task_id: str) -> str:
    """
    Strip everything after the first '€' character.
    Example:
        "Job Activities€€icon/Programming_py.png..." -> "job_activities"
        - take text before the first '€€'
        - lowercase
        - normalize accents
        - keep letters/numbers, convert spaces/dashes to underscores
        - collapse duplicate underscores
    """
    if not raw_task_id:
        return raw_task_id
    head = raw_task_id.split("€€", 1)[0]
    head = unicodedata.normalize("NFKD", head).encode("ascii", "ignore").decode("ascii")
    head = head.lower().strip()
    head = re.sub(r"[^\w\s-]", "", head)
    head = re.sub(r"[\s-]+", "_", head)
    head = re.sub(r"_+", "_", head).strip("_")
    return head

def task_entry_from_json(entry_dict: dict) -> TaskEntry:
    """Parse TaskEntry JSON into a TaskEntry ORM object."""
    props = {p["propertyName"]: p.get("value") for p in entry_dict["properties"]}

    return TaskEntry(
        task_id=clean_task_id(props["taskID"]),
        global_identifier=entry_dict["globalIdentifier"],
        finished=bool(props["finished"]),
        deleted_new=bool(props["deletedNew"]),
        notes=props.get("notes"),
        create_timestamp=ms_to_datetime(props["createTimeStamp"][1]),
        start_time=ms_to_datetime(props["startTime"][1]),
        end_time=ms_to_datetime(props["endTime"][1]) if props.get("endTime") else None,
        last_update_timestamp=ms_to_datetime(props["lastUpdateTimeStamp"][1]),
    )

def upsert_task_entries_row_by_row(entries: List[TaskEntry]) -> Tuple[List[TaskEntry], List[TaskEntry], List[TaskEntry]]:
    """
    Upsert TaskEntry rows one by one, each with its own commit.
    Returns (created, updated, unchanged).
    """
    created, updated, unchanged = [], [], []

    with SessionLocal() as s:
        for entry in entries:
            try:
                # always normalize task_id before comparing or persisting
                entry.task_id = clean_task_id(entry.task_id)

                existing = (
                    s.query(TaskEntry)
                    .filter_by(global_identifier=entry.global_identifier)
                    .one_or_none()
                )

                if existing:
                    changed = False
                    fields = [
                        "task_id",
                        "finished",
                        "deleted_new",
                        "notes",
                        "create_timestamp",
                        "start_time",
                        "end_time",
                        "last_update_timestamp",
                    ]
                    for field in fields:
                        new_val = getattr(entry, field)
                        if getattr(existing, field) != new_val:
                            setattr(existing, field, new_val)
                            changed = True

                    if changed:
                        s.commit()
                        s.refresh(existing)
                        updated.append(existing)
                    else:
                        unchanged.append(existing)

                else:
                    s.add(entry)
                    s.commit()
                    s.refresh(entry)
                    created.append(entry)

            except Exception as e:
                s.rollback()
                print(f"Skipped entry {entry.global_identifier} due to error: {e}")

    return created, updated, unchanged

