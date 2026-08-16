"""add pdm table

Revision ID: 430fdc355390
Revises: e83fc2beb574
Create Date: 2026-07-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '430fdc355390'
down_revision: Union[str, Sequence[str], None] = 'e83fc2beb574'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "pdm",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False, unique=True),
        sa.Column("source_url", sa.String(length=2048), nullable=False),
        sa.Column("pdf_url", sa.String(length=2048), nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("pdm")
