"""Module 6: Semantic Search — embedder consumer.

episode.analyzed -> Voyage embeddings -> Qdrant (episode_vectors, segment_vectors)
                  -> episode.indexed
"""

import asyncio
import logging

from sqlalchemy import select, text
from sqlalchemy.orm import selectinload

from common.config import get_settings
from common.db import session_scope
from common.kafka_backbone import run_consumer_loop
from common.models import Episode, EpisodeIntelligence, Transcript
from common.topics import EPISODE_ANALYZED, EPISODE_INDEXED
from semantic_search.qdrant_store import (
    ensure_collections,
    get_client,
    upsert_episode_vector,
    upsert_segment_vectors,
)
from semantic_search.segment_chunking import chunk_transcript_segments
from semantic_search.voyage_client import embed_document, embed_documents

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGE = "embedding"


async def handle(payload: dict) -> None:
    episode_id = payload["episode_id"]
    settings = get_settings()
    qdrant = get_client()
    await ensure_collections(qdrant)

    async with session_scope() as session:
        episode = await session.scalar(
            select(Episode).options(selectinload(Episode.podcast)).where(Episode.id == episode_id)
        )
        intelligence = await session.scalar(
            select(EpisodeIntelligence).where(EpisodeIntelligence.episode_id == episode_id)
        )
        transcript = await session.scalar(
            select(Transcript).where(Transcript.episode_id == episode_id)
        )
        if episode is None or intelligence is None or transcript is None:
            raise ValueError(f"episode {episode_id} missing intelligence or transcript")

        topic_rows = (
            await session.execute(
                text(
                    "SELECT t.name, t.slug FROM topics t "
                    "JOIN episode_topics et ON et.topic_id = t.id WHERE et.episode_id = :id"
                ),
                {"id": episode_id},
            )
        ).all()
        topic_names = [row[0] for row in topic_rows]
        topic_slugs = [row[1] for row in topic_rows]

        episode.status = "embedding"
        await session.commit()

        # Episode-level vector: title + summary + topics + keywords.
        episode_text = " ".join(
            [episode.title, intelligence.summary, *topic_names, *intelligence.keywords]
        )
        episode_vector = await embed_document(episode_text)

        published_at_ts = int(episode.published_at.timestamp()) if episode.published_at else None
        await upsert_episode_vector(
            qdrant,
            episode_id,
            episode_vector,
            payload={
                "podcast_id": episode.podcast_id,
                "topic_slugs": topic_slugs,
                "publisher": episode.podcast.publisher if episode.podcast else None,
                "published_at_ts": published_at_ts,
                "duration_seconds": episode.duration_seconds,
            },
        )

        # Segment-level vectors: batched, one call for all chunks (not per-chunk).
        chunks = chunk_transcript_segments(transcript.segments)
        chunk_vectors = await embed_documents([c.text for c in chunks])
        await upsert_segment_vectors(
            qdrant,
            episode_id,
            segments=[
                {"chunk_index": c.chunk_index, "start": c.start, "end": c.end} for c in chunks
            ],
            vectors=chunk_vectors,
        )

        episode.embedding_model_version = settings.voyage_model
        episode.status = "indexed"
        await session.commit()
        logger.info("episode_id=%s embedded, %d segment chunks", episode_id, len(chunks))


async def on_permanent_failure(payload: dict, exc: Exception) -> None:
    episode_id = payload["episode_id"]
    async with session_scope() as session:
        await session.execute(
            text(
                "UPDATE episodes SET status='embedding_failed', last_error=:err, "
                "attempts=attempts+1 WHERE id=:id"
            ),
            {"err": str(exc), "id": episode_id},
        )
        await session.commit()


async def main() -> None:
    await run_consumer_loop(
        stage=STAGE,
        input_topic=EPISODE_ANALYZED,
        group_id="embedder",
        handler=handle,
        produce_topic=EPISODE_INDEXED,
        on_permanent_failure=on_permanent_failure,
    )


if __name__ == "__main__":
    asyncio.run(main())
