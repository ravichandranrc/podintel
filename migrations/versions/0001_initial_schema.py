"""Initial schema — podcasts, episodes, transcripts, episode_intelligence, topics, episode_topics

Revision ID: 0001
Revises:
Create Date: 2026-08-10

Matches DESIGN.md §9 exactly; each table's owning module is noted inline.
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Module 1: Podcast Ingestion
    op.execute("""
        CREATE TABLE podcasts (
            id BIGSERIAL PRIMARY KEY,
            feed_url VARCHAR NOT NULL UNIQUE,
            title VARCHAR NOT NULL,
            publisher VARCHAR,
            description TEXT,
            artwork_url VARCHAR,
            language VARCHAR(10),
            is_active BOOLEAN NOT NULL DEFAULT true,
            last_polled_at TIMESTAMPTZ,
            last_poll_status VARCHAR NOT NULL DEFAULT 'pending'
                CHECK (last_poll_status IN ('pending','success','failed')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE episodes (
            id BIGSERIAL PRIMARY KEY,
            podcast_id BIGINT NOT NULL REFERENCES podcasts(id),
            guid VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            description TEXT,
            source_audio_url VARCHAR NOT NULL,
            storage_key VARCHAR,
            duration_seconds INT,
            published_at TIMESTAMPTZ,
            status VARCHAR NOT NULL DEFAULT 'discovered'
                CHECK (status IN (
                    'discovered','downloading','downloaded','download_failed',
                    'transcribing','transcribed','transcription_failed',
                    'analyzing','analyzed','analysis_failed',
                    'embedding','embedding_failed','indexed'
                )),
            attempts INT NOT NULL DEFAULT 0,
            last_error TEXT,
            embedding_model_version VARCHAR,
            search_vector TSVECTOR,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (podcast_id, guid)
        );
    """)
    op.execute("CREATE INDEX idx_episodes_status ON episodes(status);")
    op.execute("CREATE INDEX idx_episodes_published_at ON episodes(published_at DESC);")
    op.execute("CREATE INDEX idx_episodes_search_vector ON episodes USING GIN(search_vector);")

    # Module 3: Transcription
    op.execute("""
        CREATE TABLE transcripts (
            id BIGSERIAL PRIMARY KEY,
            episode_id BIGINT NOT NULL UNIQUE REFERENCES episodes(id) ON DELETE CASCADE,
            full_text TEXT NOT NULL,
            segments JSONB NOT NULL DEFAULT '[]',
            provider VARCHAR NOT NULL,
            language VARCHAR(10),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # Module 4: LLM-Based Podcast Intelligence
    op.execute("""
        CREATE TABLE episode_intelligence (
            id BIGSERIAL PRIMARY KEY,
            episode_id BIGINT NOT NULL UNIQUE REFERENCES episodes(id) ON DELETE CASCADE,
            summary TEXT NOT NULL,
            keywords TEXT[] NOT NULL DEFAULT '{}',
            model VARCHAR NOT NULL,
            prompt_version VARCHAR NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE topics (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            slug VARCHAR NOT NULL UNIQUE
        );
    """)

    op.execute("""
        CREATE TABLE episode_topics (
            episode_id BIGINT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
            topic_id BIGINT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
            PRIMARY KEY (episode_id, topic_id)
        );
    """)
    op.execute("CREATE INDEX idx_episode_topics_topic ON episode_topics(topic_id);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS episode_topics;")
    op.execute("DROP TABLE IF EXISTS topics;")
    op.execute("DROP TABLE IF EXISTS episode_intelligence;")
    op.execute("DROP TABLE IF EXISTS transcripts;")
    op.execute("DROP TABLE IF EXISTS episodes;")
    op.execute("DROP TABLE IF EXISTS podcasts;")
