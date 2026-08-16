"""add election district result summaries

Revision ID: c1e135579655
Revises: b7faa9622280
Create Date: 2026-07-27 16:54:13.625114

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c1e135579655'
down_revision: Union[str, Sequence[str], None] = 'b7faa9622280'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "parliament_election_district_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("parliament_election.id"), nullable=False),
        sa.Column("district_id", sa.Integer(), sa.ForeignKey("district.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "district_id"),
    )
    op.create_table(
        "parliament_election_district_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "district_result_id",
            sa.Integer(),
            sa.ForeignKey("parliament_election_district_result.id"),
            nullable=False,
        ),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("parliament_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("district_result_id", "party_id"),
    )

    op.create_table(
        "presidential_election_district_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("presidential_election.id"), nullable=False),
        sa.Column("district_id", sa.Integer(), sa.ForeignKey("district.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "district_id"),
    )
    op.create_table(
        "presidential_election_district_candidate_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "district_result_id",
            sa.Integer(),
            sa.ForeignKey("presidential_election_district_result.id"),
            nullable=False,
        ),
        sa.Column(
            "candidate_id", sa.Integer(), sa.ForeignKey("presidential_election_candidate.id"), nullable=False
        ),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("district_result_id", "candidate_id"),
    )

    op.create_table(
        "european_election_district_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("european_election.id"), nullable=False),
        sa.Column("district_id", sa.Integer(), sa.ForeignKey("district.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "district_id"),
    )
    op.create_table(
        "european_election_district_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "district_result_id",
            sa.Integer(),
            sa.ForeignKey("european_election_district_result.id"),
            nullable=False,
        ),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("european_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("district_result_id", "party_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("european_election_district_party_result")
    op.drop_table("european_election_district_result")
    op.drop_table("presidential_election_district_candidate_result")
    op.drop_table("presidential_election_district_result")
    op.drop_table("parliament_election_district_party_result")
    op.drop_table("parliament_election_district_result")
