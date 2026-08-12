from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/podintel"
    database_url_sync: str = "postgresql+psycopg://postgres:postgres@localhost:5432/podintel"

    kafka_bootstrap_servers: str = "localhost:9092"

    qdrant_url: str = "http://localhost:6333"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_bucket: str = "podintel-audio"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_public_endpoint_url: str = "http://localhost:9000"

    anthropic_api_key: str = ""
    claude_sonnet_model: str = "claude-sonnet-5"
    claude_haiku_model: str = "claude-haiku-4-5-20251001"

    voyage_api_key: str = ""
    voyage_model: str = "voyage-3"

    deepgram_api_key: str = ""

    admin_api_key: str = "change-me"

    feed_poll_interval_seconds: int = 1800
    pipeline_max_attempts: int = 3


@lru_cache
def get_settings() -> Settings:
    return Settings()
