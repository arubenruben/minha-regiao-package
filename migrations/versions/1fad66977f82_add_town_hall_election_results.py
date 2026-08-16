"""add town hall election results

Revision ID: 1fad66977f82
Revises: 7ce3ea03287e
Create Date: 2026-07-27 16:46:17.700257

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1fad66977f82'
down_revision: Union[str, Sequence[str], None] = '7ce3ea03287e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "town_hall_election_list",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("town_hall_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=True),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=True),
        sa.CheckConstraint(
            "(city_id IS NOT NULL) != (parish_id IS NOT NULL)",
            name="ck_town_hall_election_list_exactly_one_territory",
        ),
    )
    op.create_index(
        "ix_town_hall_election_list_election_city_name",
        "town_hall_election_list",
        ["election_id", "city_id", "name"],
        unique=True,
        postgresql_where=sa.text("city_id IS NOT NULL"),
    )
    op.create_index(
        "ix_town_hall_election_list_election_parish_name",
        "town_hall_election_list",
        ["election_id", "parish_id", "name"],
        unique=True,
        postgresql_where=sa.text("parish_id IS NOT NULL"),
    )

    op.create_table(
        "town_hall_election_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("town_hall_election.id"), nullable=False),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "parish_id"),
    )

    op.create_table(
        "town_hall_election_list_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("result_id", sa.Integer(), sa.ForeignKey("town_hall_election_result.id"), nullable=False),
        sa.Column("list_id", sa.Integer(), sa.ForeignKey("town_hall_election_list.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("result_id", "list_id"),
    )

    op.create_table(
        "town_hall_election_seat",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("town_hall_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=True),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=True),
        sa.Column("list_id", sa.Integer(), sa.ForeignKey("town_hall_election_list.id"), nullable=False),
        sa.Column("seats", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "(city_id IS NOT NULL) != (parish_id IS NOT NULL)",
            name="ck_town_hall_election_seat_exactly_one_territory",
        ),
    )
    op.create_index(
        "ix_town_hall_election_seat_election_city_list",
        "town_hall_election_seat",
        ["election_id", "city_id", "list_id"],
        unique=True,
        postgresql_where=sa.text("city_id IS NOT NULL"),
    )
    op.create_index(
        "ix_town_hall_election_seat_election_parish_list",
        "town_hall_election_seat",
        ["election_id", "parish_id", "list_id"],
        unique=True,
        postgresql_where=sa.text("parish_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_town_hall_election_seat_election_parish_list", table_name="town_hall_election_seat")
    op.drop_index("ix_town_hall_election_seat_election_city_list", table_name="town_hall_election_seat")
    op.drop_table("town_hall_election_seat")
    op.drop_table("town_hall_election_list_result")
    op.drop_table("town_hall_election_result")
    op.drop_index("ix_town_hall_election_list_election_parish_name", table_name="town_hall_election_list")
    op.drop_index("ix_town_hall_election_list_election_city_name", table_name="town_hall_election_list")
    op.drop_table("town_hall_election_list")
