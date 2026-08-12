"""Module 3: Transcription — transcriber consumer.

episode.downloaded -> STT -> transcripts row -> episode.transcribed
"""

import asyncio
import logging

from sqlalchemy import text

from audio_storage.object_storage import download_audio
from common.db import session_scope
from common.kafka_backbone import run_consumer_loop
from common.models import Episode, Transcript
from common.topics import EPISODE_DOWNLOADED, EPISODE_TRANSCRIBED
from transcription.deepgram_provider import DeepgramTranscriber
from transcription.provider import Transcriber

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGE = "transcription"


def get_transcriber() -> Transcriber:
    return DeepgramTranscriber()


async def handle(payload: dict) -> None:
    episode_id = payload["episode_id"]
    transcriber = get_transcriber()

    async with session_scope() as session:
        episode = await session.get(Episode, episode_id)
        if episode is None or not episode.storage_key:
            raise ValueError(f"episode {episode_id} has no storage_key")
        episode.status = "transcribing"
        await session.commit()

        audio_bytes = await download_audio(episode.storage_key)
        result = await transcriber.transcribe(audio_bytes, content_type="audio/mpeg")

        transcript = Transcript(
            episode_id=episode.id,
            full_text=result.full_text,
            segments=[
                {"start": s.start, "end": s.end, "text": s.text} for s in result.segments
            ],
            provider=transcriber.provider_name,
            language=result.language,
        )
        session.add(transcript)
        episode.status = "transcribed"
        await session.commit()
        logger.info(
            "episode_id=%s transcribed, %d segments, %d chars",
            episode_id, len(result.segments), len(result.full_text),
        )


async def on_permanent_failure(payload: dict, exc: Exception) -> None:
    episode_id = payload["episode_id"]
    async with session_scope() as session:
        await session.execute(
            text(
                "UPDATE episodes SET status='transcription_failed', last_error=:err, "
                "attempts=attempts+1 WHERE id=:id"
            ),
            {"err": str(exc), "id": episode_id},
        )
        await session.commit()


async def main() -> None:
    await run_consumer_loop(
        stage=STAGE,
        input_topic=EPISODE_DOWNLOADED,
        group_id="transcriber",
        handler=handle,
        produce_topic=EPISODE_TRANSCRIBED,
        on_permanent_failure=on_permanent_failure,
    )


if __name__ == "__main__":
    asyncio.run(main())
