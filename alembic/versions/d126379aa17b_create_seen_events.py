"""create seen_events

Revision ID: d126379aa17b
Revises: 
Create Date: 2025-08-31 01:37:22.648642

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0001_create_seen_events'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "seen_events",
        sa.Column("event_id", sa.Text(), primary_key=True),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("first_seen", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    # helpful query index (optional but recommended)
    op.create_index("ix_seen_user_day_endpoint", "seen_events", ["user_id", "day", "endpoint"])


def downgrade() -> None:
    op.drop_index("ix_seen_user_day_endpoint", table_name="seen_events")
    op.drop_table("seen_events")
