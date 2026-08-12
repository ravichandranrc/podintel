import datetime

from sqlalchemy import (
    ARRAY,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# Episode pipeline status values — see DESIGN.md §2 for the state machine.
EPISODE_STATUSES = (
    "discovered",
    "downloading",
    "downloaded",
    "download_failed",
    "transcribing",
    "transcribed",
    "transcription_failed",
    "analyzing",
    "analyzed",
    "analysis_failed",
    "embedding",
    "embedding_failed",
    "indexed",
)

FAILED_STATUSES = {s for s in EPISODE_STATUSES if s.endswith("_failed")}


class Base(DeclarativeBase):
    pass


class Podcast(Base):
    __tablename__ = "podcasts"

    id: Mapped[int] = mapped_column(primary_key=True)
    feed_url: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    publisher: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)
    artwork_url: Mapped[str | None] = mapped_column(String)
    language: Mapped[str | None] = mapped_column(String(10))
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    last_polled_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    last_poll_status: Mapped[str] = mapped_column(String, default="pending", nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    episodes: Mapped[list["Episode"]] = relationship(back_populates="podcast")

    __table_args__ = (
        CheckConstraint(
            "last_poll_status IN ('pending','success','failed')",
            name="ck_podcast_poll_status",
        ),
    )


class Episode(Base):
    __tablename__ = "episodes"

    id: Mapped[int] = mapped_column(primary_key=True)
    podcast_id: Mapped[int] = mapped_column(ForeignKey("podcasts.id"), nullable=False)
    guid: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    source_audio_url: Mapped[str] = mapped_column(String, nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String)
    duration_seconds: Mapped[int | None] = mapped_column(Integer)
    published_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String, default="discovered", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    embedding_model_version: Mapped[str | None] = mapped_column(String)
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    podcast: Mapped["Podcast"] = relationship(back_populates="episodes")
    transcript: Mapped["Transcript | None"] = relationship(
        back_populates="episode", uselist=False, cascade="all, delete-orphan"
    )
    intelligence: Mapped["EpisodeIntelligence | None"] = relationship(
        back_populates="episode", uselist=False, cascade="all, delete-orphan"
    )
    topic_links: Mapped[list["EpisodeTopic"]] = relationship(
        back_populates="episode", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("podcast_id", "guid", name="uq_episode_podcast_guid"),
        CheckConstraint(f"status IN {EPISODE_STATUSES!r}", name="ck_episode_status"),
        Index("idx_episodes_status", "status"),
        Index("idx_episodes_published_at", published_at.desc()),
        Index("idx_episodes_search_vector", "search_vector", postgresql_using="gin"),
    )


class Transcript(Base):
    __tablename__ = "transcripts"

    id: Mapped[int] = mapped_column(primary_key=True)
    episode_id: Mapped[int] = mapped_column(
        ForeignKey("episodes.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    full_text: Mapped[str] = mapped_column(Text, nullable=False)
    segments: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    provider: Mapped[str] = mapped_column(String, nullable=False)
    language: Mapped[str | None] = mapped_column(String(10))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    episode: Mapped["Episode"] = relationship(back_populates="transcript")


class EpisodeIntelligence(Base):
    __tablename__ = "episode_intelligence"

    id: Mapped[int] = mapped_column(primary_key=True)
    episode_id: Mapped[int] = mapped_column(
        ForeignKey("episodes.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    keywords: Mapped[list[str]] = mapped_column(ARRAY(String), default=list, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    prompt_version: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    episode: Mapped["Episode"] = relationship(back_populates="intelligence")


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    slug: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    episode_links: Mapped[list["EpisodeTopic"]] = relationship(back_populates="topic")


class EpisodeTopic(Base):
    __tablename__ = "episode_topics"

    episode_id: Mapped[int] = mapped_column(
        ForeignKey("episodes.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        ForeignKey("topics.id", ondelete="CASCADE"), primary_key=True
    )

    episode: Mapped["Episode"] = relationship(back_populates="topic_links")
    topic: Mapped["Topic"] = relationship(back_populates="episode_links")

    __table_args__ = (Index("idx_episode_topics_topic", "topic_id"),)
