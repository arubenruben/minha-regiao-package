"""add regulation extraction results and pdm documents

Revision ID: a23162f2b95c
Revises: c1e135579655
Create Date: 2026-09-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'a23162f2b95c'
down_revision: Union[str, Sequence[str], None] = 'c1e135579655'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _add_extraction_result_columns(table: str) -> None:
    op.add_column(table, sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"))
    op.add_column(table, sa.Column("raw_text", sa.Text(), nullable=True))
    op.add_column(table, sa.Column("structure", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.alter_column(table, "status", server_default=None)


def _drop_extraction_result_columns(table: str) -> None:
    op.drop_column(table, "structure")
    op.drop_column(table, "raw_text")
    op.drop_column(table, "status")


def upgrade() -> None:
    """Upgrade schema."""
    _add_extraction_result_columns("rmue")
    _add_extraction_result_columns("fee_regulation")

    op.add_column("pdm", sa.Column("title", sa.String(length=255), nullable=True))
    op.add_column("pdm", sa.Column("identifier", sa.String(length=255), nullable=True))
    op.alter_column("pdm", "pdf_url", existing_type=sa.String(length=2048), nullable=True)

    op.create_table(
        "pdm_document",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pdm_id", sa.Integer(), sa.ForeignKey("pdm.id"), nullable=False),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("doc_type", sa.String(length=255), nullable=False),
        sa.Column("number", sa.String(length=64), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("suffix", sa.Integer(), nullable=True),
        sa.Column("data_publicacao", sa.String(length=64), nullable=True),
        sa.Column("dinamica", sa.String(length=255), nullable=True),
        sa.Column("publicacao", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("structure", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.UniqueConstraint("pdm_id", "url", name="uq_pdm_document_pdm_id_url"),
    )
    op.alter_column("pdm_document", "status", server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("pdm_document")

    op.alter_column("pdm", "pdf_url", existing_type=sa.String(length=2048), nullable=False)
    op.drop_column("pdm", "identifier")
    op.drop_column("pdm", "title")

    _drop_extraction_result_columns("fee_regulation")
    _drop_extraction_result_columns("rmue")
