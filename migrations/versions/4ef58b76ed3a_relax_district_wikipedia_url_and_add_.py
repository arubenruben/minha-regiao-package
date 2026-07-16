"""relax district wikipedia_url and add unique district name

Revision ID: 4ef58b76ed3a
Revises: 275f2bb54026
Create Date: 2026-07-16 14:19:20.686980

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4ef58b76ed3a'
down_revision: Union[str, Sequence[str], None] = '275f2bb54026'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column("district", "wikipedia_url", existing_type=sa.String(length=2048), nullable=True)
    op.create_unique_constraint("uq_district_name", "district", ["name"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("uq_district_name", "district", type_="unique")
    op.alter_column("district", "wikipedia_url", existing_type=sa.String(length=2048), nullable=False)
