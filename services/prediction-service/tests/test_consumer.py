import json
import pytest

from prediction_service.main import handle_kafka_message


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
    def __init__(self, data: dict):
        self._value = json.dumps(data).encode("utf-8")

    def value(self) -> bytes:
        return self._value


class FailingProcessedEventStore(FakeProcessedEventStore):
    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event,
    ) -> None:
        raise RuntimeError("database unavailable")


def test_handle_kafka_message_commits_ignored_event():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()

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
    )

    assert consumer.committed_messages == [message]


def test_handle_kafka_message_commits_successfully_processed_route():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer()

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
    )

    assert "evt_route_commit_001" in store.processed_event_ids
    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == [message]


def test_handle_kafka_message_does_not_commit_when_processing_fails():
    store = FailingProcessedEventStore()
    consumer = FakeConsumer()

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

    with pytest.raises(RuntimeError, match="database unavailable"):
        handle_kafka_message(
            message,
            store,
            consumer,
        )

    assert consumer.committed_messages == []


def test_handle_kafka_message_handles_redelivery_after_commit_failure():
    store = FakeProcessedEventStore()
    consumer = FakeConsumer(
        commit_error=RuntimeError("commit unavailable"),
    )

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
    )

    assert len(store.outbox_events) == 1
    assert consumer.committed_messages == [message]
