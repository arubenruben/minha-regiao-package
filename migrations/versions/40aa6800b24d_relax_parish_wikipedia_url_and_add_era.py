"""relax parish wikipedia_url and add era

Revision ID: 40aa6800b24d
Revises: 4ef58b76ed3a
Create Date: 2026-07-24 15:56:10.527235

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '40aa6800b24d'
down_revision: Union[str, Sequence[str], None] = '4ef58b76ed3a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

parish_era = postgresql.ENUM("pre_2013", "post_2013", "post_2021", name="parishera")


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column("parish", "wikipedia_url", existing_type=sa.String(length=2048), nullable=True)

    parish_era.create(op.get_bind())
    op.add_column("parish", sa.Column("era", parish_era, nullable=False, server_default="post_2021"))
    op.alter_column("parish", "era", server_default=None)

    # A parish's INE code is only unique within a single territorial map
    # (era); the pre-2013, 2013-2021, and post-2021 maps can assign the same
    # code to different parishes.
    op.drop_constraint("parish_ine_code_key", "parish", type_="unique")
    op.create_unique_constraint("uq_parish_ine_code_era", "parish", ["ine_code", "era"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("uq_parish_ine_code_era", "parish", type_="unique")
    op.create_unique_constraint("parish_ine_code_key", "parish", ["ine_code"])

    op.drop_column("parish", "era")
    parish_era.drop(op.get_bind())

    op.alter_column("parish", "wikipedia_url", existing_type=sa.String(length=2048), nullable=False)
