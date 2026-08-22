import json
import os
import threading
import time

from confluent_kafka import Consumer
from pydantic import ValidationError

from prediction_service.models import RouteCalculatedEvent
from prediction_service.publisher import EventPublisher, KafkaEventPublisher
from prediction_service.prediction import handle_route_calculated
from prediction_service.store import PostgresPredictionStore
from prediction_service.outbox import publish_pending_events

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipment-events")
KAFKA_GROUP_ID = "prediction-service"

DATABASE_URL = os.getenv("DATABASE_URL")

def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # New consumer groups replay available events; existing groups resume from committed offsets.
            "auto.offset.reset": "earliest",
        }
    )

def run_outbox_publisher(
    store: PostgresPredictionStore,
    publisher: EventPublisher,
) -> None:
    while True:
        try:
            publish_pending_events(store, publisher)
        except Exception as exc:
            print(f"Failed to publish pending outbox events: {exc}")

        time.sleep(5)

def main():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    publisher = KafkaEventPublisher(
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC,
    )

    store = PostgresPredictionStore(DATABASE_URL)

    outbox_thread = threading.Thread(
        target=run_outbox_publisher,
        args=(store, publisher),
        daemon=True,
    )
    outbox_thread.start()

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka consumer error: {message.error()}")
                continue

            try:
                data = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"Invalid Kafka message, skipping: {exc}")
                continue

            # Multiple event types share the topic; this service only handles RouteCalculated.
            if data.get("event_type") != "RouteCalculated":
                continue

            try:
                event = RouteCalculatedEvent.model_validate(data)
            except ValidationError as exc:
                print(f"Invalid RouteCalculated event, skipping: {exc}")
                continue

            eta_event = handle_route_calculated(
                event,
                store,
            )

            if eta_event is None:
                continue

            print(
                f"ETA predicted and queued for shipment {eta_event.shipment_id}: "
                f"{eta_event.payload.estimated_travel_minutes} minutes"
            )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
