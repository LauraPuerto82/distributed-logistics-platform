import os
import threading
import time

from confluent_kafka import Consumer

from prediction_service.publisher import (
    EventPublisher,
    KafkaDeadLetterPublisher,
    KafkaEventPublisher,
)
from prediction_service.store import PostgresPredictionStore
from prediction_service.outbox import publish_pending_events
from prediction_service.consumer import RetryBackoff, handle_kafka_message

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "shipment-events")
KAFKA_GROUP_ID = "prediction-service"
KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "prediction-service-dlq",
)

DATABASE_URL = os.getenv("DATABASE_URL")


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # New consumer groups replay available events; existing groups resume from committed offsets.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
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

    dead_letter_publisher = KafkaDeadLetterPublisher(
        broker=KAFKA_BROKER,
        topic=KAFKA_DLQ_TOPIC,
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

            handle_kafka_message(
                message,
                store,
                consumer,
                dead_letter_publisher,
                RetryBackoff(),
            )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
