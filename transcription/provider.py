"""Module 3: Transcription — pluggable STT provider interface.

Swappable behind this interface (SPEC.md §7 / DESIGN.md §13): a provider change
never touches the transcriber consumer or anything downstream.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Segment:
    start: float
    end: float
    text: str


@dataclass
class TranscriptResult:
    full_text: str
    segments: list[Segment]
    language: str | None


class Transcriber(ABC):
    provider_name: str

    @abstractmethod
    async def transcribe(self, audio_bytes: bytes, content_type: str) -> TranscriptResult: ...
