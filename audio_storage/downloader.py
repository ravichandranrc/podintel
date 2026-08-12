"""Module 2: Audio Storage — downloader consumer.

episode.discovered -> fetch audio, PUT to object storage -> episode.downloaded
"""

import asyncio
import logging

import httpx
from sqlalchemy import text

from audio_storage.object_storage import storage_key_for, upload_audio
from common.db import session_scope
from common.kafka_backbone import run_consumer_loop
from common.models import Episode
from common.topics import EPISODE_DISCOVERED, EPISODE_DOWNLOADED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGE = "download"


async def handle(payload: dict) -> None:
    episode_id = payload["episode_id"]

    async with session_scope() as session:
        episode = await session.get(Episode, episode_id)
        if episode is None:
            raise ValueError(f"episode {episode_id} not found")
        episode.status = "downloading"
        await session.commit()

        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            response = await client.get(episode.source_audio_url)
            response.raise_for_status()
        content_type = response.headers.get("content-type", "audio/mpeg")

        storage_key = storage_key_for(episode.podcast_id, episode.id)
        await upload_audio(storage_key, response.content, content_type)

        episode.storage_key = storage_key
        episode.status = "downloaded"
        await session.commit()
        logger.info("episode_id=%s downloaded, storage_key=%s", episode_id, storage_key)


async def on_permanent_failure(payload: dict, exc: Exception) -> None:
    episode_id = payload["episode_id"]
    async with session_scope() as session:
        await session.execute(
            text(
                "UPDATE episodes SET status='download_failed', last_error=:err, "
                "attempts=attempts+1 WHERE id=:id"
            ),
            {"err": str(exc), "id": episode_id},
        )
        await session.commit()


async def main() -> None:
    await run_consumer_loop(
        stage=STAGE,
        input_topic=EPISODE_DISCOVERED,
        group_id="downloader",
        handler=handle,
        produce_topic=EPISODE_DOWNLOADED,
        on_permanent_failure=on_permanent_failure,
    )


if __name__ == "__main__":
    asyncio.run(main())
