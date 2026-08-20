import pytest
from prediction_service.models import RouteCalculatedEvent
from prediction_service.prediction import (
    handle_route_calculated,
    predict_travel_time,
    process_route_calculated,
)


class FakeEventPublisher:
    def __init__(self, error=None):
        self.published_events = []
        self.error = error

    def publish_eta_predicted(self, event):
        if self.error:
            raise self.error

        self.published_events.append(event)


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


def test_handle_route_calculated_publishes_eta_predicted():
    publisher = FakeEventPublisher()

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

    result = handle_route_calculated(route_event, publisher)

    assert len(publisher.published_events) == 1

    published_event = publisher.published_events[0]

    assert published_event.event_type == "ETAPredicted"
    assert published_event.shipment_id == "shp_123"
    assert published_event.payload.estimated_travel_minutes == 552

    assert result == published_event

def test_handle_route_calculated_propagates_publisher_error():
    publisher = FakeEventPublisher(
        error=RuntimeError("Kafka unavailable")
    )

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

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        handle_route_calculated(route_event, publisher)