"""add rmue and fee_regulation tables

Revision ID: e83fc2beb574
Revises: 2c8aae9d850d
Create Date: 2026-07-14 15:10:56.902841

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e83fc2beb574'
down_revision: Union[str, Sequence[str], None] = '2c8aae9d850d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_regulation_table(name: str) -> None:
    op.create_table(
        name,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("is_complete", sa.Boolean(), nullable=False),
        sa.Column("dre_url", sa.String(length=2048), nullable=False),
        sa.Column("pdf_url", sa.String(length=2048), nullable=True),
        sa.UniqueConstraint("city_id", "dre_url", name=f"uq_{name}_city_dre_url"),
    )


def upgrade() -> None:
    """Upgrade schema."""
    _create_regulation_table("rmue")
    _create_regulation_table("fee_regulation")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("fee_regulation")
    op.drop_table("rmue")
