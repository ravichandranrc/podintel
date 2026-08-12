"""Module 4: topic normalization — freeform Claude topics -> shared `topics` dimension.

DESIGN.md §6: case/whitespace-folded, slugified, get-or-create — "good enough"
consistency across episodes without a curated taxonomy.
"""

import re

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.models import EpisodeTopic, Topic


def slugify(name: str) -> str:
    slug = name.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


async def link_episode_topics(
    session: AsyncSession, episode_id: int, topic_names: list[str]
) -> None:
    for raw_name in topic_names:
        name = raw_name.strip()
        if not name:
            continue
        slug = slugify(name)
        if not slug:
            continue

        topic = await session.scalar(select(Topic).where(Topic.slug == slug))
        if topic is None:
            topic = Topic(name=name, slug=slug)
            session.add(topic)
            await session.flush()  # need topic.id before linking

        existing_link = await session.scalar(
            select(EpisodeTopic).where(
                EpisodeTopic.episode_id == episode_id, EpisodeTopic.topic_id == topic.id
            )
        )
        if existing_link is None:
            session.add(EpisodeTopic(episode_id=episode_id, topic_id=topic.id))
