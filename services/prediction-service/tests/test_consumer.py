import json
import pytest

from prediction_service.consumer import RetryBackoff, handle_kafka_message


class FakeProcessedEventStore:
    def __init__(self):
        self.processed_event_ids = set()
        self.outbox_events = []

    def is_processed(self, event_id: str) -> bool:
        return event_id in self.processed_event_ids

    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event,
    ) -> None:
        self.processed_event_ids.add(event_id)
        self.outbox_events.append(outbox_event)


class FakeConsumer:
    def __init__(self, commit_error=None):
        self.committed_messages = []
        self.commit_error = commit_error

    def commit(self, message, asynchronous=False):
        if self.commit_error:
            raise self.commit_error

        self.committed_messages.append(message)


class FakeKafkaMessage:
    def __init__(self, data: dict | None = None, raw_value: bytes | None = None):
        if raw_value is not None:
            self._value = raw_value
        else:
            self._value = json.dumps(data).encode("utf-8")

    def value(self) -> bytes:
        return self._value


class RetryingProcessedEventStore(FakeProcessedEventStore):
    def __init__(self, mark_errors):
        super().__init__()
        self.mark_errors = list(mark_errors)
        self.mark_calls = 0

    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event,
    ) -> None:
        self.mark_calls += 1

        if self.mark_errors:
            error = self.mark_errors.pop(0)

            if error is not None:
                raise error

        super().mark_processed_with_outbox_event(
            event_id,
            outbox_event,
        )


class FakeRetryBackoff:
    def __init__(self):
        self.attempts = []

    def wait(self, attempt: int) -> None:
        self.attempts.append(attempt)


class FailingProcessedEventStore(FakeProcessedEventStore):
    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event,
    ) -> None:
        raise RuntimeError("database unavailable")


class FakeDeadLetterPublisher:
    def __init__(self, error=None):
        self.published_messages = []
        self.reasons = []
        self.error = error

    def publish_dead_letter(self, message, reason: str) -> None:
        if self.error:
            raise self.error

        self.published_messages.append(message)
        self.reasons.append(reason)


def test_handle_kafka_message_commits_ignored_event():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        {
            "event_id": "evt_ignored_001",
            "event_type": "ShipmentCreated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_ignored_001",
            "payload": {
                "origin": "Madrid",
                "destination": "Bilbao",
                "weight": 15,
                "priority": "HIGH",
            },
        }
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert consumer.committed_messages == [message]


def test_handle_kafka_message_commits_successfully_processed_route():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        {
            "event_id": "evt_route_commit_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_route_commit_001",
            "payload": {
                "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
                "distance_km": 640,
            },
        }
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert "evt_route_commit_001" in store.processed_event_ids
    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_does_not_commit_when_retries_exhausted_and_dead_letter_fails():
    store = FailingProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher(
        error=RuntimeError("DLQ unavailable")
    )
    backoff = FakeRetryBackoff()

    message = FakeKafkaMessage(
        {
            "event_id": "evt_route_failure_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_route_failure_001",
            "payload": {
                "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
                "distance_km": 640,
            },
        }
    )

    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        handle_kafka_message(
            message,
            store,
            consumer,
            dead_letter_publisher,
            backoff,
        )

    assert dead_letter_publisher.published_messages == []
    assert consumer.committed_messages == []
    assert backoff.attempts == [1, 2]


def test_handle_kafka_message_handles_redelivery_after_commit_failure():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer(
        commit_error=RuntimeError("commit unavailable"),
    )
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        {
            "event_id": "evt_route_commit_retry_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_route_commit_retry_001",
            "payload": {
                "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
                "distance_km": 640,
            },
        }
    )

    # Processing succeeds, but committing the Kafka offset fails.
    with pytest.raises(RuntimeError, match="commit unavailable"):
        handle_kafka_message(
            message,
            store,
            consumer,
            dead_letter_publisher,
            FakeRetryBackoff(),
        )

    assert "evt_route_commit_retry_001" in store.processed_event_ids
    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == []

    # Simulate Kafka redelivering the same message later.
    consumer.commit_error = None

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_sends_malformed_json_to_dead_letter_and_commits():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        raw_value=b'{"event_id": "evt_invalid_001", invalid json}'
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert dead_letter_publisher.published_messages == [message]
    assert dead_letter_publisher.reasons == ["invalid JSON"]
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_sends_invalid_route_event_to_dead_letter_and_commits():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        data={
            "event_id": "evt_invalid_route_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_invalid_route_001",
            "payload": {
                "path": ["Madrid", "Bilbao"],
                "distance_km": -100,
            },
        }
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert dead_letter_publisher.published_messages == [message]
    assert dead_letter_publisher.reasons == ["invalid RouteCalculated event"]
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_does_not_commit_when_dead_letter_publication_fails():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher(
        error=RuntimeError("DLQ unavailable")
    )

    message = FakeKafkaMessage(
        raw_value=b'{"event_id": "evt_invalid_001", invalid json}'
    )

    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        handle_kafka_message(
            message,
            store,
            consumer,
            dead_letter_publisher,
            FakeRetryBackoff(),
        )

    assert dead_letter_publisher.published_messages == []
    assert consumer.committed_messages == []


def test_handle_kafka_message_can_republish_dead_letter_after_commit_failure():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer(commit_error=RuntimeError("commit unavailable"))
    dead_letter_publisher = FakeDeadLetterPublisher()

    message = FakeKafkaMessage(
        raw_value=b'{"event_id": "evt_invalid_002", invalid json}'
    )

    with pytest.raises(RuntimeError, match="commit unavailable"):
        handle_kafka_message(
            message,
            store,
            consumer,
            dead_letter_publisher,
            FakeRetryBackoff(),
        )

    assert dead_letter_publisher.published_messages == [message]
    assert consumer.committed_messages == []

    # Simulate Kafka redelivering the same original message.
    consumer.commit_error = None

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        FakeRetryBackoff(),
    )

    assert dead_letter_publisher.published_messages == [message, message]
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_retries_transient_processing_failure():
    store = RetryingProcessedEventStore(
        mark_errors=[
            RuntimeError("database temporarily unavailable"),
            None,
        ]
    )
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()
    backoff = FakeRetryBackoff()

    message = FakeKafkaMessage(
        data={
            "event_id": "evt_retry_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_retry_001",
            "payload": {
                "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
                "distance_km": 640,
            },
        }
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        backoff,
    )

    assert store.mark_calls == 2
    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == [message]
    assert dead_letter_publisher.published_messages == []
    assert backoff.attempts == [1]


def test_handle_kafka_message_sends_to_dead_letter_after_retries_exhausted():
    store = RetryingProcessedEventStore(
        mark_errors=[
            RuntimeError("database temporarily unavailable"),
            RuntimeError("database temporarily unavailable"),
            RuntimeError("database temporarily unavailable"),
        ]
    )
    consumer = FakeConsumer()
    dead_letter_publisher = FakeDeadLetterPublisher()
    backoff = FakeRetryBackoff()

    message = FakeKafkaMessage(
        data={
            "event_id": "evt_retry_exhausted_001",
            "event_type": "RouteCalculated",
            "timestamp": "2026-08-23T10:00:00Z",
            "shipment_id": "shp_retry_exhausted_001",
            "payload": {
                "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
                "distance_km": 640,
            },
        }
    )

    handle_kafka_message(
        message,
        store,
        consumer,
        dead_letter_publisher,
        backoff,
    )

    assert store.mark_calls == 3
    assert store.outbox_events == []

    assert dead_letter_publisher.published_messages == [message]
    assert dead_letter_publisher.reasons == ["processing retries exhausted"]

    assert consumer.committed_messages == [message]
    assert backoff.attempts == [1, 2]


def test_retry_backoff_waits_exponentially(monkeypatch):
    sleep_calls = []

    monkeypatch.setattr(
        "prediction_service.consumer.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    backoff = RetryBackoff()

    backoff.wait(1)
    backoff.wait(2)

    assert sleep_calls == [1, 2]
