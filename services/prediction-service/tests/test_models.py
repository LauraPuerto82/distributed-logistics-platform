import pytest
from datetime import datetime
from prediction_service.models import RouteCalculatedEvent
from pydantic import ValidationError


def test_route_calculated_event_parses_valid_data():
    event = RouteCalculatedEvent(
        event_id="evt_123",
        event_type="RouteCalculated",
        timestamp="2026-08-20T11:00:00Z",
        shipment_id="shp_123",
        payload={
            "path": ["Madrid", "Zaragoza", "Logroño", "Bilbao"],
            "distance_km": 640,
        },
    )

    assert event.event_id == "evt_123"
    assert event.event_type == "RouteCalculated"
    assert event.shipment_id == "shp_123"
    assert event.payload.distance_km == 640
    assert event.payload.path == [
        "Madrid",
        "Zaragoza",
        "Logroño",
        "Bilbao",
    ]
    assert isinstance(event.timestamp, datetime)

def test_route_calculated_event_rejects_negative_distance():
    with pytest.raises(ValidationError):
        RouteCalculatedEvent(
            event_id="evt_123",
            event_type="RouteCalculated",
            timestamp="2026-08-20T11:00:00Z",
            shipment_id="shp_123",
            payload={
                "path": ["Madrid", "Bilbao"],
                "distance_km": -100,
            },
        )


def test_route_calculated_event_rejects_wrong_event_type():
    with pytest.raises(ValidationError):
        RouteCalculatedEvent(
            event_id="evt_123",
            event_type="ShipmentCreated",
            timestamp="2026-08-20T11:00:00Z",
            shipment_id="shp_123",
            payload={
                "path": ["Madrid", "Bilbao"],
                "distance_km": 400,
            },
        )