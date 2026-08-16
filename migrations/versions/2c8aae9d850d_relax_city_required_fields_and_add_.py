"""relax city required fields and add contact columns

Revision ID: 2c8aae9d850d
Revises: 367c08f73328
Create Date: 2026-07-14 14:44:17.762037

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2c8aae9d850d'
down_revision: Union[str, Sequence[str], None] = '367c08f73328'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column("city", "wikipedia_url", existing_type=sa.String(length=2048), nullable=True)
    op.alter_column("city", "freguesias_pt_url", existing_type=sa.String(length=2048), nullable=True)
    op.alter_column("city", "district_id", existing_type=sa.Integer(), nullable=True)

    op.add_column("city", sa.Column("email", sa.String(length=255), nullable=True))
    op.add_column("city", sa.Column("website", sa.String(length=2048), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("city", "website")
    op.drop_column("city", "email")

    op.alter_column("city", "district_id", existing_type=sa.Integer(), nullable=False)
    op.alter_column("city", "freguesias_pt_url", existing_type=sa.String(length=2048), nullable=False)
    op.alter_column("city", "wikipedia_url", existing_type=sa.String(length=2048), nullable=False)
