import pytest
from prediction_service.models import RouteCalculatedEvent
from prediction_service.prediction import (
    predict_travel_time,
    process_route_calculated,
)


def test_predict_travel_time():
    result = predict_travel_time(640)

    assert result == 552


def test_predict_travel_time_zero_distance():
    assert predict_travel_time(0) == 0


def test_predict_travel_time_decimal_distance():
    result = predict_travel_time(125.5)

    assert result > 0


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