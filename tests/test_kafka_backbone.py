"""Unit tests for the shared retry/backoff/DLQ consumer loop (common/kafka_backbone.py),
DESIGN.md §2's "one shared function every consumer calls." No live Kafka needed —
AIOKafkaConsumer/Producer are replaced with fakes so only the branching logic
(retry vs. DLQ) is under test.
"""

import json
from unittest.mock import AsyncMock

import pytest

from common import kafka_backbone
from common.topics import PIPELINE_DLQ


class FakeMessage:
    def __init__(self, value: dict, attempt: int = 0):
        self.value = json.dumps(value).encode()
        self.headers = [("attempt", str(attempt).encode())]


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]):
        self._messages = messages
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.commit = AsyncMock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


@pytest.fixture
def fake_producer(monkeypatch):
    producer = AsyncMock()
    producer.send = AsyncMock()
    producer.send_and_wait = AsyncMock()
    producer.flush = AsyncMock()
    monkeypatch.setattr(kafka_backbone, "make_producer", AsyncMock(return_value=producer))
    return producer


def _install_consumer(monkeypatch, messages: list[FakeMessage]) -> FakeConsumer:
    consumer = FakeConsumer(messages)
    monkeypatch.setattr(kafka_backbone, "AIOKafkaConsumer", lambda *a, **kw: consumer)
    return consumer


async def test_transient_failure_reproduces_to_input_topic_with_incremented_attempt(
    monkeypatch, fake_producer
):
    _install_consumer(monkeypatch, [FakeMessage({"episode_id": 1}, attempt=0)])

    async def always_fails(payload):
        raise RuntimeError("boom")

    on_permanent_failure = AsyncMock()

    await kafka_backbone.run_consumer_loop(
        stage="test",
        input_topic="in.topic",
        group_id="g",
        handler=always_fails,
        on_permanent_failure=on_permanent_failure,
        max_attempts=3,
        backoff_seconds=(0, 0, 0),
    )

    fake_producer.send.assert_awaited_once()
    _, kwargs = fake_producer.send.call_args
    assert kwargs["headers"] == [("attempt", b"1")]
    on_permanent_failure.assert_not_awaited()
    fake_producer.send_and_wait.assert_not_awaited()


async def test_exhausted_retries_route_to_dlq_and_call_failure_hook(monkeypatch, fake_producer):
    # attempt=2 already tried twice; next_attempt=3 >= max_attempts=3 -> DLQ.
    _install_consumer(monkeypatch, [FakeMessage({"episode_id": 42}, attempt=2)])

    async def always_fails(payload):
        raise RuntimeError("still broken")

    on_permanent_failure = AsyncMock()

    await kafka_backbone.run_consumer_loop(
        stage="test",
        input_topic="in.topic",
        group_id="g",
        handler=always_fails,
        on_permanent_failure=on_permanent_failure,
        max_attempts=3,
        backoff_seconds=(0, 0, 0),
    )

    on_permanent_failure.assert_awaited_once()
    fake_producer.send_and_wait.assert_awaited_once()
    args, kwargs = fake_producer.send_and_wait.call_args
    assert args[0] == PIPELINE_DLQ
    dlq_payload = json.loads(kwargs["value"])
    assert dlq_payload == {
        "episode_id": 42,
        "stage": "test",
        "attempts": 3,
        "last_error": "still broken",
    }
    fake_producer.send.assert_not_awaited()  # no retry-produce once exhausted


async def test_success_commits_and_produces_to_next_topic(monkeypatch, fake_producer):
    _install_consumer(monkeypatch, [FakeMessage({"episode_id": 7}, attempt=0)])
    handled = AsyncMock()

    await kafka_backbone.run_consumer_loop(
        stage="test",
        input_topic="in.topic",
        group_id="g",
        handler=handled,
        produce_topic="out.topic",
        max_attempts=3,
    )

    handled.assert_awaited_once_with({"episode_id": 7})
    fake_producer.send_and_wait.assert_awaited_once()
    args, kwargs = fake_producer.send_and_wait.call_args
    assert args[0] == "out.topic"
