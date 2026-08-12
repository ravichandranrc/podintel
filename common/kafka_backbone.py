"""Shared Kafka backbone: producer helper + the retry/backoff/DLQ consumer loop.

DESIGN.md §13 calls out that per-stage retry/backoff/DLQ logic should live in one
shared function every consumer calls, rather than five hand-rolled copies. This
module is that function (`run_consumer_loop`) — each stage module (downloader,
transcriber, analyzer, embedder) supplies only its topic names and a handler.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from common.config import get_settings
from common.topics import PIPELINE_DLQ

logger = logging.getLogger(__name__)

# Backoff before each retry attempt, indexed by (attempt_number - 1).
DEFAULT_BACKOFF_SECONDS = (30, 120, 480)

Handler = Callable[[dict], Awaitable[None]]
FailureHook = Callable[[dict, Exception], Awaitable[None]]


def _headers_get(headers: list[tuple[str, bytes]], key: str, default: str) -> str:
    for k, v in headers:
        if k == key:
            return v.decode()
    return default


async def produce_event(
    producer: AIOKafkaProducer, topic: str, episode_id: int, payload: dict
) -> None:
    """Produce one pipeline event, keyed by episode_id for per-episode ordering."""
    await producer.send_and_wait(
        topic,
        key=str(episode_id).encode(),
        value=json.dumps(payload).encode(),
    )


async def make_producer() -> AIOKafkaProducer:
    settings = get_settings()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    return producer


async def run_consumer_loop(
    *,
    stage: str,
    input_topic: str,
    group_id: str,
    handler: Handler,
    produce_topic: str | None = None,
    on_permanent_failure: FailureHook | None = None,
    max_attempts: int | None = None,
    backoff_seconds: tuple[int, ...] = DEFAULT_BACKOFF_SECONDS,
) -> None:
    """Consume `input_topic`, run `handler(payload)` per message, and apply the
    shared retry/backoff/DLQ policy (DESIGN.md §2) on failure.

    `handler` does the stage's actual work (download/transcribe/analyze/embed) and
    is responsible for its own DB writes on success. It should raise on failure —
    this loop handles everything about retrying, not the handler.

    `on_permanent_failure(payload, exc)` is called once retries are exhausted, right
    before the DLQ produce, so the caller can set `episodes.status = '{stage}_failed'`
    — this loop doesn't know the caller's DB status column values.
    """
    settings = get_settings()
    max_attempts = max_attempts or settings.pipeline_max_attempts

    consumer = AIOKafkaConsumer(
        input_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = await make_producer()
    await consumer.start()
    logger.info("stage=%s consuming topic=%s group=%s", stage, input_topic, group_id)

    try:
        async for message in consumer:
            payload = json.loads(message.value.decode())
            attempt = int(_headers_get(message.headers or [], "attempt", "0"))
            episode_id = payload.get("episode_id")

            try:
                await handler(payload)
                if produce_topic:
                    await produce_event(producer, produce_topic, episode_id, payload)
                await consumer.commit()
                logger.info("stage=%s episode_id=%s ok", stage, episode_id)

            except Exception as exc:  # noqa: BLE001 — deliberately broad: any failure here is retried/DLQ'd
                next_attempt = attempt + 1
                logger.warning(
                    "stage=%s episode_id=%s attempt=%s failed: %s",
                    stage, episode_id, next_attempt, exc,
                )

                if next_attempt >= max_attempts:
                    if on_permanent_failure:
                        await on_permanent_failure(payload, exc)
                    await producer.send_and_wait(
                        PIPELINE_DLQ,
                        key=str(episode_id).encode(),
                        value=json.dumps(
                            {
                                "episode_id": episode_id,
                                "stage": stage,
                                "attempts": next_attempt,
                                "last_error": str(exc),
                            }
                        ).encode(),
                    )
                else:
                    backoff = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
                    await asyncio.sleep(backoff)
                    await producer.send(
                        input_topic,
                        key=str(episode_id).encode(),
                        value=message.value,
                        headers=[("attempt", str(next_attempt).encode())],
                    )
                    await producer.flush()

                await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
