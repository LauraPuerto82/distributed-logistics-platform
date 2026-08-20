import json
import os

from confluent_kafka import Consumer

from prediction_service.models import RouteCalculatedEvent
from prediction_service.publisher import KafkaEventPublisher
from prediction_service.prediction import handle_route_calculated

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipment-events")
KAFKA_GROUP_ID = "prediction-service"

def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # New consumer groups replay available events; existing groups resume from committed offsets.
            "auto.offset.reset": "earliest",
        }
    )

def main():
    print("Prediction Service starting...")

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    publisher = KafkaEventPublisher(
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka consumer error: {message.error()}")
                continue

            data = json.loads(message.value().decode("utf-8"))

            # Multiple event types share the topic; this service only handles RouteCalculated.
            if data.get("event_type") != "RouteCalculated":
                continue

            event = RouteCalculatedEvent.model_validate(data)
            eta_event = handle_route_calculated(
                event,
                publisher,
            )

            print(
                f"ETA predicted and published for shipment {eta_event.shipment_id}: "
                f"{eta_event.payload.estimated_travel_minutes} minutes"
            )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
