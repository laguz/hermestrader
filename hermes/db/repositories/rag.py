from __future__ import annotations

import logging
from typing import Any, Dict, List
from sqlalchemy import select
from hermes.db.orm import DoctrineEmbedding

logger = logging.getLogger("hermes.db.repositories.rag")


def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    """Calculate the cosine similarity between two vectors."""
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = sum(a * a for a in v1) ** 0.5
    norm2 = sum(b * b for b in v2) ** 0.5
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)


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
        """Perform semantic vector search to find top matching doctrine guidelines."""
        # Detect the SQL dialect (PostgreSQL vs SQLite)
        dialect_name = "sqlite"
        if session.bind:
            dialect_name = session.bind.dialect.name

        if dialect_name == "postgresql":
            # PostgreSQL pgvector operator '<=>' computes cosine distance.
            # Ascending order (lowest distance) retrieves most similar.
            q = (
                select(DoctrineEmbedding)
                .order_by(DoctrineEmbedding.embedding.op("<=>")(query_embedding))
                .limit(limit)
            )
            result = await session.execute(q)
            rows = result.scalars().all()
            return [{"text": r.guideline_text, "metadata": r.metadata_json} for r in rows]
        else:
            # SQLite fallback for testing and non-production setups: run cosine distance in memory.
            q = select(DoctrineEmbedding)
            result = await session.execute(q)
            rows = result.scalars().all()
            
            scored = []
            for r in rows:
                emb = r.embedding
                if isinstance(emb, list) and len(emb) == len(query_embedding):
                    sim = cosine_similarity(emb, query_embedding)
                    scored.append((sim, r))
            
            # Sort by similarity descending (largest first)
            scored.sort(key=lambda x: x[0], reverse=True)
            return [{"text": r.guideline_text, "metadata": r.metadata_json} for _, r in scored[:limit]]
