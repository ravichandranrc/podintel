"""Admin routes — feed registration + ops view for stuck/failed episodes.

Protected by a single static API key header, not a full user/JWT system:
feed curation is low-frequency and low-blast-radius (DESIGN.md §13).
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from common.db import get_db
from common.kafka_backbone import make_producer, produce_event
from common.models import EPISODE_STATUSES, Episode
from common.topics import RETRY_TOPIC_BY_FAILED_STATUS
from ingestion.service import register_podcast
from web.deps import require_admin

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


class AddFeedRequest(BaseModel):
    feed_url: str


@router.post("/podcasts")
async def add_feed(body: AddFeedRequest, session: AsyncSession = Depends(get_db)):
    podcast = await register_podcast(session, body.feed_url)
    return {
        "id": podcast.id,
        "title": podcast.title,
        "feed_url": podcast.feed_url,
        "last_poll_status": podcast.last_poll_status,
    }


@router.get("/episodes")
async def list_episodes(status: str | None = None, session: AsyncSession = Depends(get_db)):
    if status and status not in EPISODE_STATUSES:
        raise HTTPException(status_code=400, detail=f"unknown status {status!r}")

    query = select(Episode).order_by(Episode.updated_at.desc()).limit(100)
    if status:
        query = query.where(Episode.status == status)
    episodes = (await session.scalars(query)).all()
    return [
        {
            "id": e.id,
            "podcast_id": e.podcast_id,
            "title": e.title,
            "status": e.status,
            "attempts": e.attempts,
            "last_error": e.last_error,
            "updated_at": e.updated_at,
        }
        for e in episodes
    ]


@router.post("/episodes/{episode_id}/retry")
async def retry_episode(episode_id: int, session: AsyncSession = Depends(get_db)):
    episode = await session.get(Episode, episode_id)
    if episode is None:
        raise HTTPException(status_code=404, detail="episode not found")

    retry_topic = RETRY_TOPIC_BY_FAILED_STATUS.get(episode.status)
    if retry_topic is None:
        raise HTTPException(
            status_code=400,
            detail=f"episode status {episode.status!r} is not a retriable failed state",
        )

    await session.execute(
        text("UPDATE episodes SET attempts = 0, last_error = NULL WHERE id = :id"),
        {"id": episode_id},
    )
    await session.commit()

    producer = await make_producer()
    try:
        await produce_event(producer, retry_topic, episode_id, {"episode_id": episode_id})
    finally:
        await producer.stop()

    return {"episode_id": episode_id, "re_produced_to": retry_topic}
