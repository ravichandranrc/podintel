"""Module 6: Semantic Search — Voyage AI embeddings.

Claude (Module 4) has no embeddings endpoint, so vectorization is a separate
call to Voyage — Anthropic's recommended embeddings partner (DESIGN.md §6).
Voyage is used exclusively for vectorization; Claude is never asked to embed.
"""

import voyageai

from common.config import get_settings

settings = get_settings()


def _client() -> voyageai.AsyncClient:
    return voyageai.AsyncClient(api_key=settings.voyage_api_key)


async def embed_document(text: str) -> list[float]:
    result = await _client().embed([text], model=settings.voyage_model, input_type="document")
    return result.embeddings[0]


async def embed_documents(texts: list[str]) -> list[list[float]]:
    """Batched — one call for all chunks, not one call per chunk (DESIGN.md §5)."""
    if not texts:
        return []
    result = await _client().embed(texts, model=settings.voyage_model, input_type="document")
    return result.embeddings


async def embed_query(text: str) -> list[float]:
    result = await _client().embed([text], model=settings.voyage_model, input_type="query")
    return result.embeddings[0]
