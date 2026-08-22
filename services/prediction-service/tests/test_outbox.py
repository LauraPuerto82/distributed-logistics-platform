import pytest

from prediction_service.models import ETAPredictedEvent, ETAPredictedPayload
from prediction_service.outbox import publish_pending_events


class FakeOutboxStore:
    def __init__(self, pending_events=None, mark_error=None):
        self.pending_events = pending_events or []
        self.published_ids = []
        self.mark_error = mark_error

    def get_pending_events(self) -> list[ETAPredictedEvent]:
        return self.pending_events

    def mark_published(self, event_id: str) -> None:
        if self.mark_error:
            raise self.mark_error

        self.published_ids.append(event_id)


class FakeEventPublisher:
    def __init__(self, error=None):
        self.published_events = []
        self.error = error

    def publish_eta_predicted(self, event: ETAPredictedEvent) -> None:
        if self.error:
            raise self.error

        self.published_events.append(event)


def test_publish_pending_events_publishes_and_marks_events():
    event = ETAPredictedEvent(
        event_id="evt_eta_123",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_123",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    outbox_store = FakeOutboxStore(
        pending_events=[event],
    )
    publisher = FakeEventPublisher()

    publish_pending_events(outbox_store, publisher)

    assert len(publisher.published_events) == 1
    assert publisher.published_events[0].event_id == "evt_eta_123"

    assert outbox_store.published_ids == ["evt_eta_123"]


def test_publish_pending_events_does_not_mark_event_when_publish_fails():
    event = ETAPredictedEvent(
        event_id="evt_eta_123",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_123",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    outbox_store = FakeOutboxStore(
        pending_events=[event],
    )

    publisher = FakeEventPublisher(
        error=RuntimeError("Kafka unavailable"),
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        publish_pending_events(outbox_store, publisher)

    assert outbox_store.published_ids == []


def test_publish_pending_events_can_republish_when_mark_published_fails():
    event = ETAPredictedEvent(
        event_id="evt_eta_123",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_123",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    outbox_store = FakeOutboxStore(
        pending_events=[event],
        mark_error=RuntimeError("database unavailable"),
    )
    publisher = FakeEventPublisher()

    # Kafka publication succeeds, but marking the event as published fails.
    with pytest.raises(RuntimeError, match="database unavailable"):
        publish_pending_events(outbox_store, publisher)

    assert len(publisher.published_events) == 1

    # Simulate PostgreSQL being available again.
    outbox_store.mark_error = None

    publish_pending_events(outbox_store, publisher)

    assert len(publisher.published_events) == 2

    assert (
        publisher.published_events[0].event_id == publisher.published_events[1].event_id
    )
