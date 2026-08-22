import pytest
from prediction_service.models import RouteCalculatedEvent
from prediction_service.prediction import (
    handle_route_calculated,
    predict_travel_time,
    process_route_calculated,
)


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


def test_predict_travel_time():
    result = predict_travel_time(640)

    assert result == 552


def test_predict_travel_time_zero_distance():
    assert predict_travel_time(0) == 0


def test_predict_travel_time_decimal_distance():
    result = predict_travel_time(125.5)

    assert result == 108


def test_predict_travel_time_negative_distance():
    with pytest.raises(ValueError):
        predict_travel_time(-100)


def test_process_route_calculated_creates_eta_predicted_event():
    route_event = RouteCalculatedEvent(
        event_id="evt_route_123",
        event_type="RouteCalculated",
        timestamp="2026-08-20T11:00:00Z",
        shipment_id="shp_123",
        payload={
            "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
            "distance_km": 640,
        },
    )

    result = process_route_calculated(route_event)

    assert result.event_type == "ETAPredicted"
    assert result.shipment_id == "shp_123"
    assert result.payload.estimated_travel_minutes == 552
    assert result.event_id != route_event.event_id


def test_handle_route_calculated_stores_eta_predicted_outbox_event():
    store = FakeProcessedEventStore()

    route_event = RouteCalculatedEvent(
        event_id="evt_route_123",
        event_type="RouteCalculated",
        timestamp="2026-08-20T11:00:00Z",
        shipment_id="shp_123",
        payload={
            "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
            "distance_km": 640,
        },
    )

    result = handle_route_calculated(route_event, store)

    assert result is not None
    assert len(store.outbox_events) == 1

    stored_event = store.outbox_events[0]

    assert stored_event.event_type == "ETAPredicted"
    assert stored_event.shipment_id == "shp_123"
    assert stored_event.payload.estimated_travel_minutes == 552

    assert result == stored_event
    assert route_event.event_id in store.processed_event_ids


def test_handle_route_calculated_does_not_process_duplicate_event():
    store = FakeProcessedEventStore()

    route_event = RouteCalculatedEvent(
        event_id="evt_route_123",
        event_type="RouteCalculated",
        timestamp="2026-08-20T11:00:00Z",
        shipment_id="shp_123",
        payload={
            "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
            "distance_km": 640,
        },
    )

    first_result = handle_route_calculated(route_event, store)
    second_result = handle_route_calculated(route_event, store)

    assert first_result is not None
    assert second_result is None
    assert len(store.outbox_events) == 1
