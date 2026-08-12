"""Module 1: Podcast Ingestion — feed registration and per-poll episode dedup."""

import logging

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from common.models import Podcast
from ingestion.rss import parse_feed

logger = logging.getLogger(__name__)

_INSERT_EPISODE_SQL = text("""
    INSERT INTO episodes (podcast_id, guid, title, description, source_audio_url,
                           duration_seconds, published_at)
    VALUES (:podcast_id, :guid, :title, :description, :source_audio_url,
            :duration_seconds, :published_at)
    ON CONFLICT (podcast_id, guid) DO NOTHING
    RETURNING id
""")


async def register_podcast(session: AsyncSession, feed_url: str) -> Podcast:
    """Admin action: add a feed. Fetches it once immediately so the podcast has
    real metadata rather than a placeholder row waiting for the next poll cycle.
    """
    existing = await session.scalar(select(Podcast).where(Podcast.feed_url == feed_url))
    if existing:
        return existing

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(feed_url)
        response.raise_for_status()
    parsed_podcast, _episodes = parse_feed(response.content)

    podcast = Podcast(
        feed_url=feed_url,
        title=parsed_podcast.title,
        publisher=parsed_podcast.publisher,
        description=parsed_podcast.description,
        artwork_url=parsed_podcast.artwork_url,
        language=parsed_podcast.language,
    )
    session.add(podcast)
    await session.commit()
    await session.refresh(podcast)
    return podcast


async def poll_podcast(session: AsyncSession, podcast: Podcast) -> list[int]:
    """Fetch a feed, dedup-insert new episodes, return newly-inserted episode ids.

    `(podcast_id, guid)` is the single dedup gate (DESIGN.md §3) — a re-poll of an
    unchanged feed inserts nothing and returns an empty list, so callers never
    re-trigger the rest of the pipeline for episodes they've already seen.
    """
    new_episode_ids: list[int] = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(podcast.feed_url)
            response.raise_for_status()
        _podcast_meta, episodes = parse_feed(response.content)

        for ep in episodes:
            result = await session.execute(
                _INSERT_EPISODE_SQL,
                {
                    "podcast_id": podcast.id,
                    "guid": ep.guid,
                    "title": ep.title,
                    "description": ep.description,
                    "source_audio_url": ep.source_audio_url,
                    "duration_seconds": ep.duration_seconds,
                    "published_at": ep.published_at,
                },
            )
            row = result.first()
            if row is not None:
                new_episode_ids.append(row[0])

        podcast.last_poll_status = "success"
    except Exception:
        logger.exception("poll failed for podcast_id=%s feed_url=%s", podcast.id, podcast.feed_url)
        podcast.last_poll_status = "failed"

    await session.execute(
        text(
            "UPDATE podcasts SET last_polled_at = now(), "
            "last_poll_status = :status WHERE id = :id"
        ),
        {"status": podcast.last_poll_status, "id": podcast.id},
    )
    await session.commit()
    return new_episode_ids
