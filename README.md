# PodIntel — Podcast Intelligence Platform

PodIntel ingests podcasts from RSS feeds, transcribes episode audio, uses Claude to extract
summaries/topics/keywords, embeds content with Voyage AI for semantic retrieval, and lets users
**search, discover, and listen** through a browser-based app — by keyword *and* by meaning.

Full design and requirements: [docs/SPEC.md](docs/SPEC.md) (functional spec) and
[docs/DESIGN.md](docs/DESIGN.md) (architecture, data model, decisions).

## 1. What this is

The system is six modules chained by Kafka, plus a web app that reads from the last two:

```text
1. Podcast Ingestion  →  2. Audio Storage  →  3. Transcription  →  4. LLM-Based Podcast Intelligence
                                                                            │
                                                              ┌─────────────┴─────────────┐
                                                              ▼                           ▼
                                                       5. Search                6. Semantic Search
                                                     (keyword + filters)      (embeddings + vectors)
                                                              │                           │
                                                              └─────────────┬─────────────┘
                                                                            ▼
                                                                        Web App
```

| # | Module | What it does | Code |
|---|---|---|---|
| 1 | Podcast Ingestion | Polls curated RSS feeds every 30 min, dedups episodes by `(podcast_id, guid)` | `ingestion/` |
| 2 | Audio Storage | Downloads episode audio, stores it in S3/MinIO, serves it back via a signed URL | `audio_storage/` |
| 3 | Transcription | Audio → timestamped text via a pluggable STT provider (Deepgram by default) | `transcription/` |
| 4 | LLM-Based Podcast Intelligence | Transcript → summary/topics/keywords via Claude (Sonnet + Haiku) | `intelligence/` |
| 5 | Search | Postgres full-text keyword + topic/publisher/date/duration filters | `search/` |
| 6 | Semantic Search | Voyage AI embeddings + Qdrant vector search, hybrid-ranked on top of Module 5 | `semantic_search/` |

Each stage hands off to the next over Kafka (one topic per transition — see DESIGN.md §2/§4),
with a shared retry/backoff/dead-letter-queue policy so a slow or failing stage never blocks the
others. `web/` is the FastAPI app (search, episode/podcast pages, player, admin ops) that reads
from Postgres and Qdrant once episodes are indexed.

**Why Claude + Voyage, not one provider**: Claude has no embeddings endpoint, so Module 4 (text
understanding) uses Claude and Module 6 (vectorization) uses Voyage AI — Anthropic's recommended
embeddings partner. See DESIGN.md §13 for the full reasoning.

## 2. Setup

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package/venv manager)
- Docker with the Compose plugin (`docker compose version` should work) — or Docker Desktop, which
  includes it
- API keys for the services you want to actually exercise:
  - [Anthropic](https://console.anthropic.com/) (`ANTHROPIC_API_KEY`) — required for Module 4
  - [Voyage AI](https://www.voyageai.com/) (`VOYAGE_API_KEY`) — required for Module 6
  - [Deepgram](https://deepgram.com/) (`DEEPGRAM_API_KEY`) — required for Module 3

You can run Modules 1, 2, and 5 (ingestion, storage, keyword search) without any API keys —
they only need Postgres, Kafka, and object storage.

### Install dependencies

```bash
uv sync
```

### Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Module 4 — Claude summary/topic/keyword extraction |
| `VOYAGE_API_KEY` | Module 6 — embeddings |
| `DEEPGRAM_API_KEY` | Module 3 — speech-to-text |
| `ADMIN_API_KEY` | Protects `/admin/*` routes — set this to something real, not the default |

The rest of `.env.example` (Postgres/Kafka/Qdrant/S3 URLs) already matches the hostnames
`docker-compose.yml` uses and shouldn't need changes for local use.

## 3. Running it

### Everything, via Docker Compose

```bash
docker compose up --build
```

This starts, in order: `postgres`, `kafka`, `qdrant`, `minio` (+ bucket creation) → `migrate`
(runs Alembic) → `app` (the web server) and the five pipeline consumers (`feed-poller`,
`downloader`, `transcriber`, `analyzer`, `embedder`), each as its own container so any single
stage can be scaled independently (`docker compose up --scale transcriber=3`).

The app is then at **http://localhost:8000**.

### Register a podcast feed

Feeds are curated, not auto-discovered (see SPEC.md §3) — add one via the admin API:

```bash
curl -X POST localhost:8000/admin/podcasts \
  -H "X-Admin-Key: $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"feed_url": "https://feeds.npr.org/510289/podcast.xml"}'
```

`feed-poller` picks up new episodes on its next cycle (every `FEED_POLL_INTERVAL_SECONDS`,
default 30 min) and each episode then flows through modules 2 → 6 automatically. Track progress:

```bash
curl -H "X-Admin-Key: $ADMIN_API_KEY" "localhost:8000/admin/episodes?status=analyzing"
```

Once an episode reaches `indexed`, it's searchable at `/search` and playable at `/episodes/{id}`.

### Retrying a failed episode

If a stage exhausts its retries, the episode's status becomes `{stage}_failed` (e.g.
`transcription_failed`) and it's visible via the admin endpoint above. Retry it:

```bash
curl -X POST -H "X-Admin-Key: $ADMIN_API_KEY" localhost:8000/admin/episodes/{id}/retry
```

### Running locally without Docker (dev loop)

Useful when iterating on one module — point it at services from `docker compose up postgres kafka
qdrant minio create-bucket` and run the process directly:

```bash
docker compose up -d postgres kafka qdrant minio create-bucket
uv run alembic upgrade head
uv run uvicorn web.main:app --reload          # web app
uv run python -m ingestion.feed_poller        # module 1
uv run python -m audio_storage.downloader     # module 2
uv run python -m transcription.transcriber    # module 3
uv run python -m intelligence.analyzer        # module 4
uv run python -m semantic_search.embedder     # module 6
```

### Tests and linting

```bash
uv run pytest
uv run ruff check .
```

The test suite covers the pure-logic pieces that don't need live infrastructure: reciprocal rank
fusion (Module 6), the Kafka retry/DLQ branching logic (Module backbone), transcript chunking, and
topic slug normalization.

## 4. Project layout

```text
common/           shared: config, DB models, Kafka topics + retry/DLQ backbone
ingestion/        Module 1 — RSS parsing, feed poller
audio_storage/    Module 2 — S3/MinIO client, downloader consumer
transcription/    Module 3 — STT provider interface + Deepgram, transcriber consumer
intelligence/     Module 4 — Claude client, map-reduce chunking, analyzer consumer
search/           Module 5 — Postgres full-text keyword search
semantic_search/  Module 6 — Voyage embeddings, Qdrant, embedder consumer, hybrid ranking
web/              FastAPI app: routes, Jinja2 templates, static assets
migrations/       Alembic schema migrations
tests/            unit tests
docs/             SPEC.md and DESIGN.md
```

## 5. Current status / known gaps

This is an MVP implementation of the design in `docs/`. What's implemented and passes
lint/import/unit-test checks, but hasn't been exercised against a live Claude/Voyage/Deepgram key
or a running Kafka/Qdrant in this environment:

- Module 3 (transcription), Module 4 (Claude extraction), and Module 6 (embedding + vector search)
  — code paths are complete but need a real smoke test once you have API keys.
- What *has* been verified against a real Postgres instance and a live RSS feed: the Alembic
  migration, Module 1's fetch/parse/dedup logic end-to-end, and Module 5's keyword/topic/duration
  filtering.

Not in scope for this MVP (see SPEC.md §3/§9 for the reasoning): user accounts/login, automatic
podcast discovery, speaker diarization, full in-player topic navigation.
