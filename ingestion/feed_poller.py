"""Module 1: Podcast Ingestion — periodic feed poll loop.

Not a Kafka consumer (there's nothing upstream to consume from); it's the
producer that starts the pipeline by emitting `episode.discovered`.
"""

import asyncio
import logging

from sqlalchemy import select

from common.config import get_settings
from common.db import session_scope
from common.kafka_backbone import make_producer, produce_event
from common.models import Podcast
from common.topics import EPISODE_DISCOVERED
from ingestion.service import poll_podcast

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def poll_all_active_podcasts(producer) -> None:
    async with session_scope() as session:
        podcasts = (
            await session.scalars(select(Podcast).where(Podcast.is_active.is_(True)))
        ).all()

    for podcast in podcasts:
        async with session_scope() as session:
            podcast = await session.get(Podcast, podcast.id)
            new_episode_ids = await poll_podcast(session, podcast)

        for episode_id in new_episode_ids:
            await produce_event(
                producer,
                EPISODE_DISCOVERED,
                episode_id,
                {"episode_id": episode_id},
            )
        if new_episode_ids:
            logger.info(
                "podcast_id=%s produced %d episode.discovered event(s)",
                podcast.id, len(new_episode_ids),
            )


async def main() -> None:
    settings = get_settings()
    producer = await make_producer()
    logger.info("feed-poller started, interval=%ss", settings.feed_poll_interval_seconds)
    try:
        while True:
            try:
                await poll_all_active_podcasts(producer)
            except Exception:
                logger.exception("poll cycle failed")
            await asyncio.sleep(settings.feed_poll_interval_seconds)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
