from datetime import datetime, timezone
from uuid import uuid4

from prediction_service.models import (
    ETAPredictedEvent,
    ETAPredictedPayload,
    RouteCalculatedEvent,
)

from prediction_service.store import ProcessedEventStore

# MVP baseline: estimate travel time using an 80 km/h average speed
# plus a 15% buffer for expected operational delays.
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
    estimated_minutes = predict_travel_time(event.payload.distance_km)

    return ETAPredictedEvent(
        event_id=f"evt_{uuid4()}",
        event_type="ETAPredicted",
        timestamp=datetime.now(timezone.utc),
        shipment_id=event.shipment_id,
        payload=ETAPredictedPayload(estimated_travel_minutes=estimated_minutes),
    )


def handle_route_calculated(
    event: RouteCalculatedEvent,
    processed_event_store: ProcessedEventStore,
) -> ETAPredictedEvent | None:
    # Skip already processed input events to keep consumer processing idempotent.
    if processed_event_store.is_processed(event.event_id):
        return None

    eta_event = process_route_calculated(event)

    processed_event_store.mark_processed_with_outbox_event(
        event.event_id,
        eta_event,
    )

    return eta_event
