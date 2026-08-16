"""add european election results

Revision ID: 20d2c63afbde
Revises: 40aa6800b24d
Create Date: 2026-07-27 16:45:36.970567

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20d2c63afbde'
down_revision: Union[str, Sequence[str], None] = '40aa6800b24d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "european_election_party",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("european_election.id"), nullable=False),
        sa.UniqueConstraint("election_id", "name"),
    )

    op.create_table(
        "european_election_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("european_election.id"), nullable=False),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=True),
        sa.Column("consulate_id", sa.Integer(), sa.ForeignKey("consulate.id"), nullable=True),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "(parish_id IS NOT NULL) != (consulate_id IS NOT NULL)",
            name="ck_european_election_result_exactly_one_territory",
        ),
    )
    op.create_index(
        "ix_european_election_result_election_parish",
        "european_election_result",
        ["election_id", "parish_id"],
        unique=True,
        postgresql_where=sa.text("parish_id IS NOT NULL"),
    )
    op.create_index(
        "ix_european_election_result_election_consulate",
        "european_election_result",
        ["election_id", "consulate_id"],
        unique=True,
        postgresql_where=sa.text("consulate_id IS NOT NULL"),
    )

    op.create_table(
        "european_election_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("result_id", sa.Integer(), sa.ForeignKey("european_election_result.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("european_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("result_id", "party_id"),
    )

    op.create_table(
        "european_election_party_seat",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("european_election.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("european_election_party.id"), nullable=False),
        sa.Column("seats", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "party_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("european_election_party_seat")
    op.drop_table("european_election_party_result")
    op.drop_index("ix_european_election_result_election_consulate", table_name="european_election_result")
    op.drop_index("ix_european_election_result_election_parish", table_name="european_election_result")
    op.drop_table("european_election_result")
    op.drop_table("european_election_party")
