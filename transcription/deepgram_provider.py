"""Module 3: Transcription — Deepgram implementation of the Transcriber interface."""

import httpx

from common.config import get_settings
from transcription.provider import Segment, Transcriber, TranscriptResult

DEEPGRAM_LISTEN_URL = "https://api.deepgram.com/v1/listen"


class DeepgramTranscriber(Transcriber):
    provider_name = "deepgram"

    def __init__(self) -> None:
        self._settings = get_settings()

    async def transcribe(self, audio_bytes: bytes, content_type: str) -> TranscriptResult:
        params = {
            "model": "nova-2",
            "punctuate": "true",
            "utterances": "true",
            "smart_format": "true",
        }
        headers = {
            "Authorization": f"Token {self._settings.deepgram_api_key}",
            "Content-Type": content_type,
        }
        async with httpx.AsyncClient(timeout=600) as client:
            response = await client.post(
                DEEPGRAM_LISTEN_URL, params=params, headers=headers, content=audio_bytes
            )
            response.raise_for_status()
        data = response.json()

        channel = data["results"]["channels"][0]
        alternative = channel["alternatives"][0]
        full_text = alternative.get("transcript", "")
        language = channel.get("detected_language")

        segments = [
            Segment(start=u["start"], end=u["end"], text=u["transcript"])
            for u in data["results"].get("utterances", [])
        ]
        if not segments and full_text:
            # provider didn't return utterance-level timestamps — fall back to a
            # single whole-transcript segment so downstream chunking still works.
            words = alternative.get("words", [])
            duration = words[-1].get("end", 0.0) if words else 0.0
            segments = [Segment(start=0.0, end=duration, text=full_text)]

        return TranscriptResult(full_text=full_text, segments=segments, language=language)
