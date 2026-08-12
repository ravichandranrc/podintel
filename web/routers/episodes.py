from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from audio_storage.object_storage import signed_playback_url
from common.db import get_db
from common.models import Episode, EpisodeTopic
from web.templates_env import templates

router = APIRouter(prefix="/episodes", tags=["episodes"])


@router.get("/{episode_id}")
async def episode_detail(
    episode_id: int, request: Request, t: int | None = None, session: AsyncSession = Depends(get_db)
):
    episode = await session.scalar(
        select(Episode)
        .options(
            selectinload(Episode.podcast),
            selectinload(Episode.transcript),
            selectinload(Episode.intelligence),
            selectinload(Episode.topic_links).selectinload(EpisodeTopic.topic),
        )
        .where(Episode.id == episode_id)
    )
    if episode is None:
        raise HTTPException(status_code=404, detail="episode not found")

    topics = [link.topic.name for link in episode.topic_links]
    return templates.TemplateResponse(
        request,
        "episode_detail.html",
        {
            "episode": episode,
            "podcast": episode.podcast,
            "transcript": episode.transcript,
            "intelligence": episode.intelligence,
            "topics": topics,
            "seek_to": t,
        },
    )


@router.get("/{episode_id}/audio")
async def episode_audio(episode_id: int, session: AsyncSession = Depends(get_db)):
    episode = await session.get(Episode, episode_id)
    if episode is None or not episode.storage_key:
        raise HTTPException(status_code=404, detail="audio not available yet")
    return RedirectResponse(signed_playback_url(episode.storage_key), status_code=302)
