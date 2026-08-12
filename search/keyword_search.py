"""Module 5: Search — keyword + metadata filters over Module 4's search_vector.

DESIGN.md §7: a single Postgres query. This module alone satisfies keyword/
topic/publisher/date/duration search with no extra infrastructure — Module 6
extends it, it doesn't replace it.
"""

import datetime
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class SearchResult:
    episode_id: int
    title: str
    description: str | None
    published_at: datetime.datetime | None
    duration_seconds: int | None
    podcast_id: int
    podcast_title: str
    publisher: str | None
    topics: list[str]
    rank: float = 0.0


@dataclass
class SearchFilters:
    q: str | None = None
    topic: str | None = None
    publisher: str | None = None
    date_from: datetime.datetime | None = None
    date_to: datetime.datetime | None = None
    duration_min: int | None = None
    duration_max: int | None = None
    limit: int = 20
    offset: int = 0


def _base_query(filters: SearchFilters) -> tuple[str, dict]:
    clauses = ["e.status = 'indexed'"]
    params: dict = {}

    if filters.q:
        clauses.append("e.search_vector @@ websearch_to_tsquery('english', :q)")
        params["q"] = filters.q
    if filters.topic:
        clauses.append(
            "EXISTS (SELECT 1 FROM episode_topics et2 JOIN topics t2 ON t2.id = et2.topic_id "
            "WHERE et2.episode_id = e.id AND t2.slug = :topic)"
        )
        params["topic"] = filters.topic
    if filters.publisher:
        clauses.append("p.publisher ILIKE :publisher")
        params["publisher"] = f"%{filters.publisher}%"
    if filters.date_from:
        clauses.append("e.published_at >= :date_from")
        params["date_from"] = filters.date_from
    if filters.date_to:
        clauses.append("e.published_at <= :date_to")
        params["date_to"] = filters.date_to
    if filters.duration_min:
        clauses.append("e.duration_seconds >= :duration_min")
        params["duration_min"] = filters.duration_min
    if filters.duration_max:
        clauses.append("e.duration_seconds <= :duration_max")
        params["duration_max"] = filters.duration_max

    return " AND ".join(clauses), params


async def keyword_search(session: AsyncSession, filters: SearchFilters) -> list[SearchResult]:
    where_sql, params = _base_query(filters)
    rank_expr = (
        "ts_rank(e.search_vector, websearch_to_tsquery('english', :q))" if filters.q else "0"
    )

    sql = text(f"""
        SELECT e.id, e.title, e.description, e.published_at, e.duration_seconds,
               p.id AS podcast_id, p.title AS podcast_title, p.publisher,
               {rank_expr} AS rank,
               COALESCE(array_agg(DISTINCT t.name)
                        FILTER (WHERE t.name IS NOT NULL), '{{}}') AS topics
        FROM episodes e
        JOIN podcasts p ON p.id = e.podcast_id
        LEFT JOIN episode_topics et ON et.episode_id = e.id
        LEFT JOIN topics t ON t.id = et.topic_id
        WHERE {where_sql}
        GROUP BY e.id, p.id
        ORDER BY rank DESC, e.published_at DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)
    params["limit"] = filters.limit
    params["offset"] = filters.offset

    rows = (await session.execute(sql, params)).mappings().all()
    return [
        SearchResult(
            episode_id=row["id"],
            title=row["title"],
            description=row["description"],
            published_at=row["published_at"],
            duration_seconds=row["duration_seconds"],
            podcast_id=row["podcast_id"],
            podcast_title=row["podcast_title"],
            publisher=row["publisher"],
            topics=list(row["topics"] or []),
            rank=float(row["rank"]),
        )
        for row in rows
    ]
