from datetime import datetime, timezone

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, Date, DateTime, Boolean, PrimaryKeyConstraint, UniqueConstraint, func


class Base(DeclarativeBase):
    pass


class SeenEvent(Base):
    __tablename__ = "seen_events"

    user_id: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[object] = mapped_column(Date, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    first_seen: Mapped[object] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "date", "endpoint", name="event_id"),
    )


class Metric(Base):
    __tablename__ = "metric"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[object] = mapped_column(Date, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        # prevents duplicates on re-runs; lets us UPSERT cleanly
        UniqueConstraint("user_id", "date", "endpoint", "name", name="ux_metric_dedupe"),
    )

    def __repr__(self):
        return (f"<Metric(id={self.id}, "
                f"user_id='{self.user_id}', "
                f"date={self.date}, "
                f"endpoint='{self.endpoint}', "
                f"name='{self.name}', "
                f"value={self.value})>")


def ms_to_datetime(ms: float) -> datetime:
    """Convert millisecond timestamp (from JSON) to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class TaskEntry(Base):
    __tablename__ = "task_entry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    task_id: Mapped[str] = mapped_column(String, index=True, nullable=False)
    global_identifier: Mapped[str] = mapped_column(String, nullable=False, unique=True)

    finished: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    deleted_new: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[str] = mapped_column(String, nullable=True)

    create_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    last_update_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("global_identifier", name="ux_task_entry_global_identifier"),
    )

    def __repr__(self) -> str:
        return (
            f"<TaskEntry(id={self.id}, task_id='{self.task_id}', "
            f"global_identifier='{self.global_identifier}', "
            f"finished={self.finished}, deleted_new={self.deleted_new}, "
            f"start_time={self.start_time}, end_time={self.end_time})>"
        )
