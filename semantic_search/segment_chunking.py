"""Module 6: aggregate STT-provided segments into ~250-400 word embedding chunks.

DESIGN.md §6: chunk granularity is what lets a search result answer "which part
of this episode matched," without needing full LLM-based topic segmentation.
"""

from dataclasses import dataclass

TARGET_WORDS_PER_CHUNK = 300


@dataclass
class EmbeddingChunk:
    chunk_index: int
    start: float
    end: float
    text: str


def chunk_transcript_segments(
    segments: list[dict], target_words: int = TARGET_WORDS_PER_CHUNK
) -> list[EmbeddingChunk]:
    chunks: list[EmbeddingChunk] = []
    buffer_texts: list[str] = []
    buffer_start: float | None = None
    buffer_end: float | None = None
    word_count = 0

    def flush() -> None:
        nonlocal buffer_texts, buffer_start, buffer_end, word_count
        if buffer_texts:
            chunks.append(
                EmbeddingChunk(
                    chunk_index=len(chunks),
                    start=buffer_start or 0.0,
                    end=buffer_end or 0.0,
                    text=" ".join(buffer_texts),
                )
            )
        buffer_texts, buffer_start, buffer_end, word_count = [], None, None, 0

    for seg in segments:
        text = seg.get("text", "")
        if not text:
            continue
        if buffer_start is None:
            buffer_start = seg["start"]
        buffer_end = seg["end"]
        buffer_texts.append(text)
        word_count += len(text.split())
        if word_count >= target_words:
            flush()
    flush()

    return chunks
