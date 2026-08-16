"""add regional election results

Revision ID: 7ce3ea03287e
Revises: 20d2c63afbde
Create Date: 2026-07-27 16:45:59.326325

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7ce3ea03287e'
down_revision: Union[str, Sequence[str], None] = '20d2c63afbde'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "regional_election_party",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("regional_election.id"), nullable=False),
        sa.UniqueConstraint("election_id", "name"),
    )

    op.create_table(
        "regional_election_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("regional_election.id"), nullable=False),
        sa.Column("parish_id", sa.Integer(), sa.ForeignKey("parish.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "parish_id"),
    )

    op.create_table(
        "regional_election_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("result_id", sa.Integer(), sa.ForeignKey("regional_election_result.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("regional_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("result_id", "party_id"),
    )

    op.create_table(
        "regional_election_party_seat",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("regional_election.id"), nullable=False),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("regional_election_party.id"), nullable=False),
        sa.Column("seats", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "party_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("regional_election_party_seat")
    op.drop_table("regional_election_party_result")
    op.drop_table("regional_election_result")
    op.drop_table("regional_election_party")
