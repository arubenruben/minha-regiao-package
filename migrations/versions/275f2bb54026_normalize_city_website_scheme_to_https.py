"""normalize city website scheme to https

Revision ID: 275f2bb54026
Revises: 430fdc355390
Create Date: 2026-07-16 11:02:54.026673

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '275f2bb54026'
down_revision: Union[str, Sequence[str], None] = '430fdc355390'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        UPDATE city
        SET website = regexp_replace(website, '^http://', 'https://', 'i')
        WHERE website ILIKE 'http://%'
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Not reversible: we can't distinguish websites that were originally
    # https from those upgraded by this migration.
    pass
