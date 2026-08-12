"""RSS feed parsing — Module 1: Podcast Ingestion."""

import datetime
from dataclasses import dataclass

import feedparser


@dataclass
class ParsedPodcast:
    title: str
    publisher: str | None
    description: str | None
    artwork_url: str | None
    language: str | None


@dataclass
class ParsedEpisode:
    guid: str
    title: str
    description: str | None
    source_audio_url: str
    duration_seconds: int | None
    published_at: datetime.datetime | None


def _parse_duration(raw: str | None) -> int | None:
    """iTunes duration is either raw seconds or HH:MM:SS / MM:SS."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.isdigit():
        return int(raw)
    parts = raw.split(":")
    try:
        parts_int = [int(p) for p in parts]
    except ValueError:
        return None
    seconds = 0
    for part in parts_int:
        seconds = seconds * 60 + part
    return seconds


def _published_at(entry) -> datetime.datetime | None:
    parsed = entry.get("published_parsed")
    if not parsed:
        return None
    return datetime.datetime(*parsed[:6], tzinfo=datetime.UTC)


def _audio_enclosure_url(entry) -> str | None:
    for link in entry.get("links", []):
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("audio"):
            return link.get("href")
    # fall back to any enclosure regardless of declared mime type
    for link in entry.get("links", []):
        if link.get("rel") == "enclosure":
            return link.get("href")
    return None


def parse_feed(raw_feed: bytes | str) -> tuple[ParsedPodcast, list[ParsedEpisode]]:
    parsed = feedparser.parse(raw_feed)
    feed = parsed.feed

    podcast = ParsedPodcast(
        title=feed.get("title", "Untitled Podcast"),
        publisher=feed.get("author") or feed.get("itunes_author"),
        description=feed.get("subtitle") or feed.get("description"),
        artwork_url=(feed.get("image", {}) or {}).get("href") or feed.get("itunes_image", {}).get(
            "href"
        )
        if isinstance(feed.get("itunes_image"), dict)
        else None,
        language=feed.get("language"),
    )

    episodes: list[ParsedEpisode] = []
    for entry in parsed.entries:
        audio_url = _audio_enclosure_url(entry)
        guid = entry.get("id") or entry.get("guid") or audio_url
        if not audio_url or not guid:
            continue  # not a playable episode entry — skip
        episodes.append(
            ParsedEpisode(
                guid=guid,
                title=entry.get("title", "Untitled Episode"),
                description=entry.get("summary"),
                source_audio_url=audio_url,
                duration_seconds=_parse_duration(entry.get("itunes_duration")),
                published_at=_published_at(entry),
            )
        )
    return podcast, episodes
