import json
import time

from pydantic import ValidationError

from prediction_service.models import RouteCalculatedEvent
from prediction_service.prediction import handle_route_calculated
from prediction_service.publisher import DeadLetterPublisher
from prediction_service.store import PostgresPredictionStore

MAX_PROCESSING_ATTEMPTS = 3


class RetryBackoff:
    def wait(self, attempt: int) -> None:
        time.sleep(2 ** (attempt - 1))


def handle_kafka_message(
    message,
    store: PostgresPredictionStore,
    consumer,
    dead_letter_publisher: DeadLetterPublisher,
    backoff: RetryBackoff,
) -> None:
    try:
        data = json.loads(message.value().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        dead_letter_publisher.publish_dead_letter(
            message,
            "invalid JSON",
        )
        consumer.commit(
            message=message,
            asynchronous=False,
        )
        return

    # Multiple event types share the topic; this service only handles RouteCalculated.
    if data.get("event_type") != "RouteCalculated":
        consumer.commit(message=message, asynchronous=False)
        return

    try:
        event = RouteCalculatedEvent.model_validate(data)
    except ValidationError:
        dead_letter_publisher.publish_dead_letter(
            message,
            "invalid RouteCalculated event",
        )
        consumer.commit(
            message=message,
            asynchronous=False,
        )
        return

    for attempt in range(1, MAX_PROCESSING_ATTEMPTS + 1):
        try:
            eta_event = handle_route_calculated(
                event,
                store,
            )
            break
        except Exception:
            if attempt == MAX_PROCESSING_ATTEMPTS:
                dead_letter_publisher.publish_dead_letter(
                    message,
                    "processing retries exhausted",
                )
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )
                return

            backoff.wait(attempt)

    # Commit the Kafka offset only after processing has completed successfully.
    # If the commit fails, persistent event-id idempotency makes redelivery safe.
    consumer.commit(message=message, asynchronous=False)

    if eta_event is None:
        return

    print(
        f"ETA predicted and queued for shipment {eta_event.shipment_id}: "
        f"{eta_event.payload.estimated_travel_minutes} minutes"
    )
