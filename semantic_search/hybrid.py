"""Module 6: Semantic Search — hybrid ranking on top of Module 5.

DESIGN.md §8: keyword (Module 5) and vector paths run in parallel, always, merged
via reciprocal rank fusion; the segment lookup only runs for the already-ranked
top-N, not the whole candidate set.
"""

import datetime
from dataclasses import dataclass

from qdrant_client import AsyncQdrantClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from search.keyword_search import SearchFilters, SearchResult, keyword_search
from semantic_search.qdrant_store import best_segment_for_episode, search_episode_vectors
from semantic_search.voyage_client import embed_query

RRF_K = 60


@dataclass
class HybridResult:
    result: SearchResult
    relevant_segment: tuple[float, float] | None  # (start, end) seconds, or None


def _epoch(dt: datetime.datetime | None) -> int | None:
    return int(dt.timestamp()) if dt else None


def reciprocal_rank_fusion(
    ranked_id_lists: list[list[int]], k: int = RRF_K
) -> dict[int, float]:
    """score = sum(1 / (k + rank)) across each ranked list an id appears in.

    Pulled out as a pure function (DESIGN.md §8) so the fusion math is testable
    without a live Postgres/Qdrant — the two callers just pass in id order.
    """
    scores: dict[int, float] = {}
    for ranked_ids in ranked_id_lists:
        for rank, item_id in enumerate(ranked_ids):
            scores[item_id] = scores.get(item_id, 0.0) + 1 / (k + rank)
    return scores


async def _fetch_display_data(
    session: AsyncSession, episode_ids: list[int]
) -> dict[int, SearchResult]:
    if not episode_ids:
        return {}
    rows = (
        await session.execute(
            text("""
                SELECT e.id, e.title, e.description, e.published_at, e.duration_seconds,
                       p.id AS podcast_id, p.title AS podcast_title, p.publisher,
                       COALESCE(array_agg(DISTINCT t.name)
                                FILTER (WHERE t.name IS NOT NULL), '{}') AS topics
                FROM episodes e
                JOIN podcasts p ON p.id = e.podcast_id
                LEFT JOIN episode_topics et ON et.episode_id = e.id
                LEFT JOIN topics t ON t.id = et.topic_id
                WHERE e.id = ANY(:ids)
                GROUP BY e.id, p.id
            """),
            {"ids": episode_ids},
        )
    ).mappings().all()
    return {
        row["id"]: SearchResult(
            episode_id=row["id"],
            title=row["title"],
            description=row["description"],
            published_at=row["published_at"],
            duration_seconds=row["duration_seconds"],
            podcast_id=row["podcast_id"],
            podcast_title=row["podcast_title"],
            publisher=row["publisher"],
            topics=list(row["topics"] or []),
        )
        for row in rows
    }


async def hybrid_search(
    session: AsyncSession, qdrant: AsyncQdrantClient, filters: SearchFilters
) -> list[HybridResult]:
    keyword_results = await keyword_search(session, filters)
    keyword_by_id = {r.episode_id: r for r in keyword_results}

    query_vector: list[float] | None = None
    vector_hits: list = []
    if filters.q:
        query_vector = await embed_query(filters.q)
        vector_hits = await search_episode_vectors(
            qdrant,
            query_vector,
            limit=filters.limit,
            topic_slugs=[filters.topic] if filters.topic else None,
            publisher=filters.publisher,
            date_from=_epoch(filters.date_from),
            date_to=_epoch(filters.date_to),
            duration_min=filters.duration_min,
            duration_max=filters.duration_max,
        )

    fused_scores = reciprocal_rank_fusion(
        [
            [r.episode_id for r in keyword_results],
            [int(point.id) for point in vector_hits],
        ]
    )

    ranked_ids = sorted(fused_scores, key=lambda eid: fused_scores[eid], reverse=True)
    ranked_ids = ranked_ids[: filters.limit]

    missing_ids = [eid for eid in ranked_ids if eid not in keyword_by_id]
    display_by_id = {**keyword_by_id, **(await _fetch_display_data(session, missing_ids))}

    results: list[HybridResult] = []
    for episode_id in ranked_ids:
        base = display_by_id.get(episode_id)
        if base is None:
            continue  # episode vanished between vector index and DB read — skip, don't error
        base.rank = fused_scores[episode_id]

        relevant_segment = None
        if query_vector is not None:
            segment_point = await best_segment_for_episode(qdrant, episode_id, query_vector)
            if segment_point is not None:
                relevant_segment = (segment_point.payload["start"], segment_point.payload["end"])

        results.append(HybridResult(result=base, relevant_segment=relevant_segment))

    return results
