from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from common.db import get_db

router = APIRouter(tags=["topics"])


@router.get("/topics")
async def popular_topics(limit: int = 20, session: AsyncSession = Depends(get_db)):
    rows = (
        await session.execute(
            text("""
                SELECT t.name, t.slug, COUNT(*) AS episode_count
                FROM topics t
                JOIN episode_topics et ON et.topic_id = t.id
                GROUP BY t.id
                ORDER BY episode_count DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
    ).mappings().all()
    return [dict(row) for row in rows]
