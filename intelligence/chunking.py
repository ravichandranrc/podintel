"""Module 4: map-reduce for transcripts that exceed a practical single-call budget.

DESIGN.md §5: only activates for oversized transcripts. Below the threshold, the
transcript goes straight into the one Sonnet extraction call.
"""

from intelligence.claude_client import ClaudeClient

# ~60k chars is comfortably within Sonnet's context window with room for the
# response and instructions, but a 90+ minute episode transcript can exceed it —
# that's the case this map-reduce path exists for.
SINGLE_CALL_CHAR_THRESHOLD = 60_000
CHUNK_CHAR_SIZE = 12_000


def _chunk_text(full_text: str, chunk_size: int = CHUNK_CHAR_SIZE) -> list[str]:
    return [full_text[i : i + chunk_size] for i in range(0, len(full_text), chunk_size)]


async def prepare_extraction_input(full_text: str, claude: ClaudeClient) -> str:
    """Returns the text to hand to the final Sonnet extraction call: the
    transcript itself if it fits, otherwise a synthesis of Haiku chunk summaries.
    """
    if len(full_text) <= SINGLE_CALL_CHAR_THRESHOLD:
        return full_text

    chunks = _chunk_text(full_text)
    chunk_summaries = [await claude.summarize_chunk(chunk) for chunk in chunks]
    return "\n\n".join(f"[Segment {i + 1}] {s}" for i, s in enumerate(chunk_summaries))
