from datetime import datetime, timezone
from uuid import uuid4

from prediction_service.models import (
    ETAPredictedEvent,
    ETAPredictedPayload,
    RouteCalculatedEvent,
)

AVERAGE_SPEED_KMH = 80
BUFFER_FACTOR = 1.15


def predict_travel_time(distance_km: float) -> int:
    if distance_km < 0:
        raise ValueError("distance_km must be non-negative")

    base_hours = distance_km / AVERAGE_SPEED_KMH
    estimated_hours = base_hours * BUFFER_FACTOR

    return round(estimated_hours * 60)

def process_route_calculated(
    event: RouteCalculatedEvent,
) -> ETAPredictedEvent:
    estimated_minutes = predict_travel_time(
        event.payload.distance_km
    )

    return ETAPredictedEvent(
        event_id=f"evt_{uuid4()}",
        event_type="ETAPredicted",
        timestamp=datetime.now(timezone.utc),
        shipment_id=event.shipment_id,
        payload=ETAPredictedPayload(
            estimated_travel_minutes=estimated_minutes
        ),
    )