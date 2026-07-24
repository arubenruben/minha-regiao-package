"""add parliament election results

Revision ID: 367c08f73328
Revises: 75d148a3770d
Create Date: 2026-07-13 13:22:57.888561

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '367c08f73328'
down_revision: Union[str, Sequence[str], None] = '75d148a3770d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "electoral_circle",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("district_id", sa.Integer(), sa.ForeignKey("district.id"), nullable=True, unique=True),
    )

    op.add_column(
        "country",
        sa.Column("electoral_circle_id", sa.Integer(), sa.ForeignKey("electoral_circle.id"), nullable=True),
    )

    op.create_table(
        "parliament_election_party",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("parliament_election.id"), nullable=False),
        sa.UniqueConstraint("election_id", "name"),
    )

    op.create_table(
        "parliament_election_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("parliament_election.id"), nullable=False),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=True),
        sa.Column("consulate_id", sa.Integer(), sa.ForeignKey("consulate.id"), nullable=True),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "(parish_id IS NOT NULL) != (consulate_id IS NOT NULL)",
            name="ck_parliament_election_result_exactly_one_territory",
        ),
    )
    op.create_index(
        "ix_parliament_election_result_election_parish",
        "parliament_election_result",
        ["election_id", "parish_id"],
        unique=True,
        postgresql_where=sa.text("parish_id IS NOT NULL"),
    )
    op.create_index(
        "ix_parliament_election_result_election_consulate",
        "parliament_election_result",
        ["election_id", "consulate_id"],
        unique=True,
        postgresql_where=sa.text("consulate_id IS NOT NULL"),
    )

    op.create_table(
        "parliament_election_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("result_id", sa.Integer(), sa.ForeignKey("parliament_election_result.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("parliament_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("result_id", "party_id"),
    )

    op.create_table(
        "parliament_election_circle_seat",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("parliament_election.id"), nullable=False),
        sa.Column("circle_id", sa.Integer(), sa.ForeignKey("electoral_circle.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("parliament_election_party.id"), nullable=False),
        sa.Column("seats", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "circle_id", "party_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("parliament_election_circle_seat")
    op.drop_table("parliament_election_party_result")
    op.drop_index("ix_parliament_election_result_election_consulate", table_name="parliament_election_result")
    op.drop_index("ix_parliament_election_result_election_parish", table_name="parliament_election_result")
    op.drop_table("parliament_election_result")
    op.drop_table("parliament_election_party")
    op.drop_column("country", "electoral_circle_id")
    op.drop_table("electoral_circle")
