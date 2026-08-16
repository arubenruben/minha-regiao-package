"""add election city result summaries

Revision ID: b7faa9622280
Revises: 1fad66977f82
Create Date: 2026-07-27 16:46:40.983158

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7faa9622280'
down_revision: Union[str, Sequence[str], None] = '1fad66977f82'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "parliament_election_city_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("parliament_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "city_id"),
    )
    op.create_table(
        "parliament_election_city_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "city_result_id", sa.Integer(), sa.ForeignKey("parliament_election_city_result.id"), nullable=False
        ),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("parliament_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("city_result_id", "party_id"),
    )

    op.create_table(
        "presidential_election_city_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("presidential_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "city_id"),
    )
    op.create_table(
        "presidential_election_city_candidate_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "city_result_id", sa.Integer(), sa.ForeignKey("presidential_election_city_result.id"), nullable=False
        ),
        sa.Column(
            "candidate_id", sa.Integer(), sa.ForeignKey("presidential_election_candidate.id"), nullable=False
        ),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("city_result_id", "candidate_id"),
    )

    op.create_table(
        "european_election_city_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("european_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "city_id"),
    )
    op.create_table(
        "european_election_city_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "city_result_id", sa.Integer(), sa.ForeignKey("european_election_city_result.id"), nullable=False
        ),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("european_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("city_result_id", "party_id"),
    )

    op.create_table(
        "regional_election_city_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("regional_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "city_id"),
    )
    op.create_table(
        "regional_election_city_party_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "city_result_id", sa.Integer(), sa.ForeignKey("regional_election_city_result.id"), nullable=False
        ),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("regional_election_party.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("city_result_id", "party_id"),
    )

    op.create_table(
        "town_hall_election_city_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("election_id", sa.Integer(), sa.ForeignKey("town_hall_election.id"), nullable=False),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("city.id"), nullable=False),
        sa.Column("registered_voters", sa.Integer(), nullable=False),
        sa.Column("voters", sa.Integer(), nullable=False),
        sa.Column("blank_votes", sa.Integer(), nullable=False),
        sa.Column("null_votes", sa.Integer(), nullable=False),
        sa.Column("valid_votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("election_id", "city_id"),
    )
    op.create_table(
        "town_hall_election_city_list_result",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "city_result_id", sa.Integer(), sa.ForeignKey("town_hall_election_city_result.id"), nullable=False
        ),
        sa.Column("list_id", sa.Integer(), sa.ForeignKey("town_hall_election_list.id"), nullable=False),
        sa.Column("votes", sa.Integer(), nullable=False),
        sa.UniqueConstraint("city_result_id", "list_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("town_hall_election_city_list_result")
    op.drop_table("town_hall_election_city_result")
    op.drop_table("regional_election_city_party_result")
    op.drop_table("regional_election_city_result")
    op.drop_table("european_election_city_party_result")
    op.drop_table("european_election_city_result")
    op.drop_table("presidential_election_city_candidate_result")
    op.drop_table("presidential_election_city_result")
    op.drop_table("parliament_election_city_party_result")
    op.drop_table("parliament_election_city_result")
