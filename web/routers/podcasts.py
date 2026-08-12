from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from common.db import get_db
from common.models import Podcast
from web.templates_env import templates

router = APIRouter(prefix="/podcasts", tags=["podcasts"])


@router.get("/{podcast_id}")
async def podcast_detail(
    podcast_id: int, request: Request, session: AsyncSession = Depends(get_db)
):
    podcast = await session.scalar(
        select(Podcast).options(selectinload(Podcast.episodes)).where(Podcast.id == podcast_id)
    )
    if podcast is None:
        raise HTTPException(status_code=404, detail="podcast not found")

    episodes = sorted(
        podcast.episodes, key=lambda e: e.published_at or e.created_at, reverse=True
    )
    return templates.TemplateResponse(
        request, "podcast_detail.html", {"podcast": podcast, "episodes": episodes}
    )
