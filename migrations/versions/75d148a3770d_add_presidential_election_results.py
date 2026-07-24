"""add presidential election results

Revision ID: 75d148a3770d
Revises: bc3fde578479
Create Date: 2026-07-13 13:14:44.371366

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '75d148a3770d'
down_revision: Union[str, Sequence[str], None] = 'bc3fde578479'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "country",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
    )

    op.create_table(
        "consulate",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("code", sa.String(length=10), nullable=False, unique=True),
        sa.Column("country_id", sa.Integer(), sa.ForeignKey("country.id"), nullable=False),
    )

    op.create_table(
        "presidential_election_candidate",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("presidential_election.id"), nullable=False),
        sa.UniqueConstraint("election_id", "name"),
    )

    op.create_table(
        "presidential_election_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("presidential_election.id"), nullable=False),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=True),
        sa.Column("consulate_id", sa.Integer(), sa.ForeignKey("consulate.id"), nullable=True),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "(parish_id IS NOT NULL) != (consulate_id IS NOT NULL)",
            name="ck_presidential_election_result_exactly_one_territory",
        ),
    )
    # Partial unique indexes rather than a single UNIQUE(election_id, parish_id, consulate_id):
    # Postgres treats NULLs as distinct in unique constraints, so that wouldn't stop two rows
    # for the same election+parish as long as consulate_id is NULL on both.
    op.create_index(
        "ix_presidential_election_result_election_parish",
        "presidential_election_result",
        ["election_id", "parish_id"],
        unique=True,
        postgresql_where=sa.text("parish_id IS NOT NULL"),
    )
    op.create_index(
        "ix_presidential_election_result_election_consulate",
        "presidential_election_result",
        ["election_id", "consulate_id"],
        unique=True,
        postgresql_where=sa.text("consulate_id IS NOT NULL"),
    )

    op.create_table(
        "presidential_election_candidate_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("result_id", sa.Integer(), sa.ForeignKey("presidential_election_result.id"), nullable=False),
        sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("presidential_election_candidate.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("result_id", "candidate_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("presidential_election_candidate_result")
    op.drop_index("ix_presidential_election_result_election_consulate", table_name="presidential_election_result")
    op.drop_index("ix_presidential_election_result_election_parish", table_name="presidential_election_result")
    op.drop_table("presidential_election_result")
    op.drop_table("presidential_election_candidate")
    op.drop_table("consulate")
    op.drop_table("country")
