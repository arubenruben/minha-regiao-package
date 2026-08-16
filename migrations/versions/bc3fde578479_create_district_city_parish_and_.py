"""create district city parish and election tables

Revision ID: bc3fde578479
Revises:
Create Date: 2026-07-13 13:04:08.288872

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bc3fde578479'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_election_table(name: str, *extra_columns: sa.Column) -> None:
    op.create_table(
        name,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        *extra_columns,
    )


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "district",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("wikipedia_url", sa.String(length=2048), nullable=False),
    )

    op.create_table(
        "city",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("wikipedia_url", sa.String(length=2048), nullable=False),
        sa.Column("freguesias_pt_url", sa.String(length=2048), nullable=False),
        sa.Column("ine_code", sa.String(length=10), nullable=False, unique=True),
        sa.Column("district_id", sa.Integer(), sa.ForeignKey("district.id"), nullable=False),
    )

    op.create_table(
        "parish",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("wikipedia_url", sa.String(length=2048), nullable=False),
        sa.Column("ine_code", sa.String(length=10), nullable=False, unique=True),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
    )

    _create_election_table(
        "presidential_election",
        sa.Column("round", sa.String(length=12), nullable=False),
    )
    _create_election_table("parliament_election")
    _create_election_table("european_election")
    _create_election_table(
        "town_hall_election",
        sa.Column("sub_type", sa.String(length=15), nullable=False),
    )
    _create_election_table(
        "regional_election",
        sa.Column("sub_type", sa.String(length=7), nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("regional_election")
    op.drop_table("town_hall_election")
    op.drop_table("european_election")
    op.drop_table("parliament_election")
    op.drop_table("presidential_election")

    op.drop_table("parish")
    op.drop_table("city")
    op.drop_table("district")
