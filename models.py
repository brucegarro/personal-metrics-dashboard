from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Date, DateTime, PrimaryKeyConstraint, UniqueConstraint, func


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