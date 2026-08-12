import datetime

from fastapi import APIRouter, Depends, Request
from qdrant_client import AsyncQdrantClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from common.db import get_db
from search.keyword_search import SearchFilters
from semantic_search.hybrid import hybrid_search
from web.deps import get_qdrant
from web.templates_env import templates

router = APIRouter(tags=["search"])


async def _popular_topics(session: AsyncSession, limit: int = 12) -> list[dict]:
    rows = (
        await session.execute(
            text("""
                SELECT t.name, t.slug, COUNT(*) AS episode_count
                FROM topics t JOIN episode_topics et ON et.topic_id = t.id
                GROUP BY t.id ORDER BY episode_count DESC LIMIT :limit
            """),
            {"limit": limit},
        )
    ).mappings().all()
    return [dict(row) for row in rows]


@router.get("/")
async def home(request: Request, session: AsyncSession = Depends(get_db)):
    topics = await _popular_topics(session)
    return templates.TemplateResponse(request, "home.html", {"topics": topics})


@router.get("/search")
async def search(
    request: Request,
    q: str | None = None,
    topic: str | None = None,
    publisher: str | None = None,
    date_from: datetime.date | None = None,
    date_to: datetime.date | None = None,
    duration_min: int | None = None,
    duration_max: int | None = None,
    page: int = 1,
    session: AsyncSession = Depends(get_db),
    qdrant: AsyncQdrantClient = Depends(get_qdrant),
):
    page_size = 20
    filters = SearchFilters(
        q=q,
        topic=topic,
        publisher=publisher,
        date_from=datetime.datetime.combine(date_from, datetime.time.min) if date_from else None,
        date_to=datetime.datetime.combine(date_to, datetime.time.max) if date_to else None,
        duration_min=duration_min,
        duration_max=duration_max,
        limit=page_size,
        offset=(max(page, 1) - 1) * page_size,
    )
    results = await hybrid_search(session, qdrant, filters)

    return templates.TemplateResponse(
        request,
        "search_results.html",
        {
            "results": results,
            "query": q or "",
            "filters": filters,
            "page": page,
        },
    )
