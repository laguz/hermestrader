from __future__ import annotations

import logging
from typing import Any, Dict, List
from sqlalchemy import select
from hermes.db.orm import DoctrineEmbedding

logger = logging.getLogger("hermes.db.repositories.rag")


class RAGRepository:
    """Repository methods for RAG-based vector database storing operator guidelines."""

    @staticmethod
    async def add_guideline_embedding(
        session,
        text: str,
        embedding: List[float],
        metadata: Dict[str, Any]
    ) -> DoctrineEmbedding:
        """Insert a new guideline chunk with its embedding vector."""
        row = DoctrineEmbedding(
            guideline_text=text,
            embedding=embedding,
            metadata_json=metadata
        )
        session.add(row)
        logger.info("[RAG] Inserted guideline embedding (dim=%d, text=%s...)", len(embedding), text[:30])
        return row

    @staticmethod
    async def search_doctrine(
        session,
        query_embedding: List[float],
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Perform semantic vector search to find top matching doctrine guidelines.

        Uses the pgvector cosine-distance operator ``<=>``; ascending order
        (lowest distance) retrieves the most similar guidelines.
        """
        q = (
            select(DoctrineEmbedding)
            .order_by(DoctrineEmbedding.embedding.op("<=>")(query_embedding))
            .limit(limit)
        )
        result = await session.execute(q)
        rows = result.scalars().all()
        return [{"text": r.guideline_text, "metadata": r.metadata_json} for r in rows]
