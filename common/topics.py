"""Kafka topic names — one per pipeline stage transition. See DESIGN.md §2."""

EPISODE_DISCOVERED = "episode.discovered"  # Module 1 -> Module 2
EPISODE_DOWNLOADED = "episode.downloaded"  # Module 2 -> Module 3
EPISODE_TRANSCRIBED = "episode.transcribed"  # Module 3 -> Module 4
EPISODE_ANALYZED = "episode.analyzed"  # Module 4 -> Module 6
EPISODE_INDEXED = "episode.indexed"  # Module 6 -> (reserved for future consumers)

PIPELINE_DLQ = "pipeline.dlq"

ALL_TOPICS = (
    EPISODE_DISCOVERED,
    EPISODE_DOWNLOADED,
    EPISODE_TRANSCRIBED,
    EPISODE_ANALYZED,
    EPISODE_INDEXED,
    PIPELINE_DLQ,
)

# A manual retry (admin ops action) re-produces to whichever topic the failed
# stage originally consumed from — the same input that stage would see on a
# normal Kafka redelivery. See DESIGN.md §2/§10.
RETRY_TOPIC_BY_FAILED_STATUS = {
    "download_failed": EPISODE_DISCOVERED,
    "transcription_failed": EPISODE_DOWNLOADED,
    "analysis_failed": EPISODE_TRANSCRIBED,
    "embedding_failed": EPISODE_ANALYZED,
}
