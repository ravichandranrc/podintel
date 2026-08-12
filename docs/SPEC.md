# PodIntel — Functional Spec (MVP)

Companion to [DESIGN.md](./DESIGN.md), which covers architecture, data flow, and design trade-offs.

## 1. Overview

PodIntel is a **Podcast Intelligence Platform**: it ingests podcasts from RSS feeds via a Kafka-driven pipeline, transcribes episode audio, uses Claude to extract topics/keywords/summaries, embeds content for semantic retrieval, and lets users **search, discover, and listen** through a browser-based app — by keyword *and* by meaning. The differentiator over a plain podcast player is that search operates on *what was actually said*, not just titles and descriptions, and can match a natural-language query even when the exact words aren't in the transcript.

This spec covers **Phase 1 (MVP)** only, scoped from the source requirements doc's own phase breakdown, expanded to pull semantic search and an event-streaming backbone forward into MVP scope. Remaining Phase 2/3 items (auto-discovery, diarization, full in-episode topic navigation, personalization) are listed in §9 as roadmap, not committed scope.

## 2. Goals

- Ingest episodes from a curated set of RSS feeds without manual audio handling, via an event-driven (Kafka) pipeline that decouples each processing stage.
- Turn raw audio into structured, searchable intelligence (transcript, summary, topics, keywords, embeddings).
- Let users find the right episode by topic, keyword, publisher, date, or duration — not just by podcast name.
- Let users search by **meaning**, not just literal keyword overlap (e.g. "how companies deploy AI agents in production" should surface episodes discussing "agentic AI in prod" even without that exact phrase).
- Let users listen in-browser and read the transcript alongside the summary.
- Keep the pipeline cheap and idempotent: never re-download, re-transcribe, re-analyze, or re-embed an episode that's already been processed.

## 3. Non-goals (MVP)

- **No automatic podcast discovery.** Feeds are added to a curated list (by URL); crawling podcast directories (iTunes/Spotify/PodcastIndex) is Phase 2.
- **No user accounts, login, or personalization.** The app is publicly browsable. Playback position is remembered client-side (`localStorage`), not server-side per-user state.
- **No speaker diarization.** Transcripts are undiarized text with timestamps; "Speaker 1/2" labels are Phase 2, contingent on STT provider support.
- **No full in-player "Jump to Topic" navigation.** Topics/summary are per-episode, not LLM-labeled per time-range, in the MVP. Search results *do* surface an approximate matching timestamp (via segment-level embeddings, §5.6) — that's a byproduct of semantic search, not the same as browsing an episode's own topic map.

## 4. Core User Journey (MVP)

```text
Admin adds RSS feed
      ↓
Feed poller discovers new episodes (dedup by GUID) → Kafka: episode.discovered
      ↓
Audio downloaded → object storage → Kafka: episode.downloaded
      ↓
Speech-to-text → transcript (with timestamps) → Kafka: episode.transcribed
      ↓
Claude → summary + topics + keywords → Kafka: episode.analyzed
      ↓
Embeddings generated (episode-level + transcript-chunk-level) → vector DB → Kafka: episode.indexed
      ↓
Postgres full-text index + vector index both ready
      ↓
User searches (keyword and/or natural language) / browses in the web app
      ↓
User listens + reads transcript/summary, jumps to the matching segment
```

## 5. Functional Requirements

### 5.1 Podcast Ingestion
- Admin can register a podcast by RSS feed URL.
- A scheduled poller re-fetches each active feed on an interval and parses `<item>` entries for: episode title, description, publish date, duration, enclosure (audio) URL, episode GUID.
- Episodes are deduplicated by `(podcast_id, guid)` — a re-poll of an unchanged feed inserts nothing.
- Each episode tracks a pipeline `status` (discovered → downloaded → transcribed → analyzed → embedded → indexed) and surfaces failures per stage for retry.

### 5.2 Audio Storage
- Downloaded audio is stored in S3-compatible object storage, keyed `podcasts/{podcast_id}/episodes/{episode_id}/audio.mp3`.
- The app never proxies audio bytes through itself — episode playback uses a signed/pre-signed URL (or public bucket read, depending on deployment) issued by the API.

### 5.3 Transcription
- Every downloaded episode is sent to a speech-to-text provider.
- Output is stored as full text plus timestamped segments (`start`, `end`, `text`), one row per episode.
- Failed transcriptions retry with backoff (bounded attempts) before being marked `transcription_failed` and surfaced for manual retry.

### 5.4 Claude-Based Intelligence
- Each transcript is sent to Claude in a single tool-use call (forced JSON schema) returning: `summary`, `topics[]`, `keywords[]`.
- Long transcripts (beyond the model's practical context budget) are map-reduced: chunked and summarized with the cheaper Claude Haiku model, then a final Claude Sonnet pass synthesizes the episode-level summary/topics/keywords from the chunk summaries.
- Topics are normalized (case/whitespace-folded, slugified) and upserted into a shared `topics` dimension table so the same topic across episodes is filterable/countable, not just a free-text label.

### 5.5 Embeddings & Semantic Indexing
- Claude has no embeddings endpoint, so vectorization uses **Voyage AI** (Anthropic's recommended embeddings partner) instead — a separate call from every Claude call in this pipeline.
- Once an episode is analyzed, two embeddings are generated and written to a vector DB:
  - **Episode-level**: one vector per episode, embedding `title + summary + topics + keywords`. Used for whole-episode semantic ranking and topic/publisher/date/duration filtering.
  - **Segment-level**: one vector per transcript chunk (~250–400 words, derived from the STT-provided timestamped segments), tagged with `episode_id`, `start`, `end`. Used to surface *which part* of an episode matched a query, not just that it matched.
- Embedding is idempotent and only triggered once per `(episode_id, embedding_model_version)` — a model upgrade can be re-run deliberately without re-embedding everything else.

### 5.6 Search
Users can search/filter by:

| Criterion | MVP support |
|---|---|
| Free-text keyword (title, description, summary, transcript) | Yes — Postgres full-text search, ranked |
| Natural-language / semantic query | Yes — embedding similarity search, merged with keyword results |
| Topic | Yes — exact match against normalized `topics`, also usable as a semantic-search filter |
| Podcast / publisher | Yes — filter |
| Date range | Yes — `published_at` range filter |
| Duration | Yes — `duration_seconds` range filter |
| Speaker | No (Phase 2, needs diarization) |

A semantic query (e.g. *"podcasts discussing how companies are deploying AI agents in production"*) returns episodes whose content is conceptually related even without literal keyword overlap, each with an approximate matching timestamp range pulled from the best-scoring segment.

### 5.7 Web Application
- **Home**: search box + popular topics (top N by episode count).
- **Search results**: paginated list, each result shows podcast, title, topics, duration, publish date, and a snippet.
- **Episode detail**: audio player, summary, topic chips, full transcript (with timestamps, non-interactive in MVP).
- **Podcast detail**: podcast metadata + its episode list.

### 5.8 Podcast Player
- Play/pause, seek, volume, playback speed, progress bar — standard HTML5 `<audio>` control surface.
- Resume-from-last-position via `localStorage` keyed by episode id (no server-side state needed since there's no login in MVP).
- A search result's matching segment timestamp is a deep link (`/episodes/{id}?t=1292`) that seeks the player on load.

## 6. Data Model (summary — full DDL in DESIGN.md)

Postgres: `podcasts`, `episodes` (pipeline status, dedup key, search vector), `transcripts` (full text + timestamped segments), `episode_intelligence` (summary, keywords, model/prompt version), `topics` + `episode_topics` (normalized topic dimension).

Vector DB: `episode_vectors` (one point per episode, filterable by topic/publisher/date/duration), `segment_vectors` (one point per transcript chunk, carries `episode_id`/`start`/`end`).

## 7. Tech Stack

| Concern | Choice |
|---|---|
| Backend | FastAPI |
| Package manager | uv (`pyproject.toml` + `uv.lock`) |
| Database | PostgreSQL (full-text search via `tsvector`/GIN, combined with vector search for hybrid ranking) |
| Migrations | Alembic, raw-SQL mode |
| Object storage | S3-compatible (MinIO for local dev) |
| Vector DB | Qdrant — episode-level and segment-level embeddings, metadata filtering |
| Event backbone | Kafka — one topic per pipeline stage transition, consumer-group-per-stage, retry + DLQ topics |
| Speech-to-text | Pluggable provider interface (e.g. Deepgram, AssemblyAI) behind a common `Transcriber` abstraction |
| LLM | Anthropic Claude API — Sonnet for structured intelligence extraction, Haiku for cheap chunk-summarization |
| Embeddings | Voyage AI — Claude has no embeddings endpoint; Voyage is Anthropic's recommended pairing |
| Frontend | Jinja2 server-rendered + vanilla JS player |

## 8. Definition of Done (MVP)

- [ ] Admin can add a feed URL and see its episodes appear within one poll cycle.
- [ ] An added episode reaches `indexed` status without manual intervention, flowing entirely through Kafka stage-to-stage.
- [ ] A failed stage (download/transcribe/analyze/embed) retries automatically via the retry topic and lands in the DLQ (surfaced in an ops view) if it exhausts retries.
- [ ] Search returns relevant results for keyword, topic, publisher, date-range, and duration filters, individually and combined.
- [ ] A natural-language query (no literal keyword overlap with the transcript) still surfaces the right episode, with an approximate matching segment timestamp.
- [ ] An episode page plays audio, shows summary/topics, and displays the full transcript.
- [ ] Re-polling a feed with no new episodes does not re-trigger download/transcription/analysis/embedding for existing episodes.

## 9. Roadmap (not in MVP scope)

| Phase | Adds |
|---|---|
| Phase 2 | Automatic podcast discovery (directory APIs), speaker diarization, LLM-labeled timestamped topic segments + in-player "Jump to Topic" navigation |
| Phase 3 | Cross-podcast topic analysis / trending topics, "ask questions about an episode", personalized recommendations, daily/weekly digest |
