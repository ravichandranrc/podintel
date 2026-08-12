"""Module 4: LLM-Based Podcast Intelligence — analyzer consumer.

episode.transcribed -> Claude -> episode_intelligence + topics + search_vector
                     -> episode.analyzed
"""

import asyncio
import logging

from sqlalchemy import select, text

from common.db import session_scope
from common.kafka_backbone import run_consumer_loop
from common.models import Episode, EpisodeIntelligence, Transcript
from common.topics import EPISODE_ANALYZED, EPISODE_TRANSCRIBED
from intelligence.chunking import prepare_extraction_input
from intelligence.claude_client import PROMPT_VERSION, ClaudeClient
from intelligence.topics import link_episode_topics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGE = "analysis"

_UPDATE_SEARCH_VECTOR_SQL = text("""
    UPDATE episodes SET search_vector =
        setweight(to_tsvector('english', coalesce(:title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(:topics_keywords, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(:summary, '')), 'C') ||
        setweight(to_tsvector('english', coalesce(:transcript, '')), 'D')
    WHERE id = :id
""")


async def handle(payload: dict) -> None:
    episode_id = payload["episode_id"]
    claude = ClaudeClient()

    async with session_scope() as session:
        episode = await session.get(Episode, episode_id)
        transcript = await session.scalar(
            select(Transcript).where(Transcript.episode_id == episode_id)
        )
        if episode is None or transcript is None:
            raise ValueError(f"episode {episode_id} has no transcript")
        episode.status = "analyzing"
        await session.commit()

        extraction_input = await prepare_extraction_input(transcript.full_text, claude)
        result = await claude.extract_intelligence(extraction_input)

        existing = await session.scalar(
            select(EpisodeIntelligence).where(EpisodeIntelligence.episode_id == episode_id)
        )
        if existing:
            existing.summary = result.summary
            existing.keywords = result.keywords
            existing.model = result.model
            existing.prompt_version = PROMPT_VERSION
        else:
            session.add(
                EpisodeIntelligence(
                    episode_id=episode_id,
                    summary=result.summary,
                    keywords=result.keywords,
                    model=result.model,
                    prompt_version=PROMPT_VERSION,
                )
            )

        await link_episode_topics(session, episode_id, result.topics)

        await session.execute(
            _UPDATE_SEARCH_VECTOR_SQL,
            {
                "title": episode.title,
                "topics_keywords": " ".join(result.topics + result.keywords),
                "summary": result.summary,
                "transcript": transcript.full_text,
                "id": episode_id,
            },
        )

        episode.status = "analyzed"
        await session.commit()
        logger.info(
            "episode_id=%s analyzed, %d topics, %d keywords",
            episode_id, len(result.topics), len(result.keywords),
        )


async def on_permanent_failure(payload: dict, exc: Exception) -> None:
    episode_id = payload["episode_id"]
    async with session_scope() as session:
        await session.execute(
            text(
                "UPDATE episodes SET status='analysis_failed', last_error=:err, "
                "attempts=attempts+1 WHERE id=:id"
            ),
            {"err": str(exc), "id": episode_id},
        )
        await session.commit()


async def main() -> None:
    await run_consumer_loop(
        stage=STAGE,
        input_topic=EPISODE_TRANSCRIBED,
        group_id="analyzer",
        handler=handle,
        produce_topic=EPISODE_ANALYZED,
        on_permanent_failure=on_permanent_failure,
    )


if __name__ == "__main__":
    asyncio.run(main())
