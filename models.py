from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, Date, DateTime, PrimaryKeyConstraint, UniqueConstraint, func


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
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[object] = mapped_column(Date, nullable=False)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        # prevents duplicates on re-runs; lets us UPSERT cleanly
        UniqueConstraint("user_id", "date", "endpoint", "name", name="ux_metric_dedupe"),
    )