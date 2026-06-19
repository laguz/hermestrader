"""drop doctrine_embeddings — remove the unwired pgvector RAG subsystem

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-19

The ``doctrine_embeddings`` table backed a RAG / vector-search feature
(``RAGRepository``) that was never wired into the application: nothing computed
embeddings to populate it and nothing queried it, and the repository was never
mounted on ``HermesDB``. It was provisioned only at runtime via ``create_all``
on databases that had the pgvector ``vector`` extension.

This migration drops the table (and its standalone sequence) where present,
completing removal of the ``pgvector`` dependency. It is a no-op on a fresh
database built from the current ORM, which no longer declares the table.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS doctrine_embeddings")
    op.execute("DROP SEQUENCE IF EXISTS doctrine_embeddings_id_seq")


def downgrade() -> None:
    raise NotImplementedError(
        "doctrine_embeddings backed a removed feature and its column type "
        "requires the dropped pgvector dependency; recreate is unsupported."
    )
