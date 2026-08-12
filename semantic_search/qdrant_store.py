"""Module 6: Semantic Search — Qdrant collections.

Two collections (DESIGN.md §6), both written only by the embedder consumer:
- episode_vectors: id=episode_id, answers "which episodes are about this?"
- segment_vectors: id="{episode_id}:{chunk_index}", answers "which part?"
Both upserted deterministically by id — retries/re-embeds are clean overwrites.
"""

from qdrant_client import AsyncQdrantClient, models

from common.config import get_settings

EPISODE_VECTORS = "episode_vectors"
SEGMENT_VECTORS = "segment_vectors"
VECTOR_SIZE = 1024  # voyage-3 embedding dimension


def get_client() -> AsyncQdrantClient:
    return AsyncQdrantClient(url=get_settings().qdrant_url)


async def ensure_collections(client: AsyncQdrantClient) -> None:
    for name in (EPISODE_VECTORS, SEGMENT_VECTORS):
        if not await client.collection_exists(name):
            await client.create_collection(
                collection_name=name,
                vectors_config=models.VectorParams(
                    size=VECTOR_SIZE, distance=models.Distance.COSINE
                ),
            )


async def upsert_episode_vector(
    client: AsyncQdrantClient,
    episode_id: int,
    vector: list[float],
    payload: dict,
) -> None:
    await client.upsert(
        collection_name=EPISODE_VECTORS,
        points=[models.PointStruct(id=episode_id, vector=vector, payload=payload)],
    )


async def upsert_segment_vectors(
    client: AsyncQdrantClient,
    episode_id: int,
    segments: list[dict],
    vectors: list[list[float]],
) -> None:
    """`segments` are {"start", "end", "chunk_index"} dicts, positionally matched to `vectors`."""
    points = [
        models.PointStruct(
            id=f"{episode_id}:{seg['chunk_index']}",
            vector=vector,
            payload={"episode_id": episode_id, "start": seg["start"], "end": seg["end"]},
        )
        for seg, vector in zip(segments, vectors, strict=True)
    ]
    if points:
        await client.upsert(collection_name=SEGMENT_VECTORS, points=points)


def _build_filter(
    topic_slugs: list[str] | None = None,
    publisher: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    duration_min: int | None = None,
    duration_max: int | None = None,
) -> models.Filter | None:
    must: list[models.Condition] = []
    if topic_slugs:
        must.append(
            models.FieldCondition(key="topic_slugs", match=models.MatchAny(any=topic_slugs))
        )
    if publisher:
        must.append(
            models.FieldCondition(key="publisher", match=models.MatchValue(value=publisher))
        )
    if date_from or date_to:
        must.append(
            models.FieldCondition(
                key="published_at_ts",
                range=models.Range(gte=date_from, lte=date_to),
            )
        )
    if duration_min or duration_max:
        must.append(
            models.FieldCondition(
                key="duration_seconds",
                range=models.Range(gte=duration_min, lte=duration_max),
            )
        )
    return models.Filter(must=must) if must else None


async def search_episode_vectors(
    client: AsyncQdrantClient,
    query_vector: list[float],
    limit: int = 20,
    **filter_kwargs,
) -> list[models.ScoredPoint]:
    response = await client.query_points(
        collection_name=EPISODE_VECTORS,
        query=query_vector,
        query_filter=_build_filter(**filter_kwargs),
        limit=limit,
    )
    return response.points


async def best_segment_for_episode(
    client: AsyncQdrantClient, episode_id: int, query_vector: list[float]
) -> models.ScoredPoint | None:
    response = await client.query_points(
        collection_name=SEGMENT_VECTORS,
        query=query_vector,
        query_filter=models.Filter(
            must=[
                models.FieldCondition(key="episode_id", match=models.MatchValue(value=episode_id))
            ]
        ),
        limit=1,
    )
    return response.points[0] if response.points else None
