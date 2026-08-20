from typing import Protocol

from confluent_kafka import Producer

from prediction_service.models import ETAPredictedEvent


class EventPublisher(Protocol):
    def publish_eta_predicted(self, event: ETAPredictedEvent) -> None: ...


class KafkaEventPublisher:
    def __init__(self, broker: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": broker,
            }
        )

    def publish_eta_predicted(self, event: ETAPredictedEvent) -> None:
        payload = event.model_dump_json()

        self._producer.produce(
            topic=self._topic,
            key=event.shipment_id,
            value=payload,
        )

        # Synchronous flush keeps delivery semantics simple for the MVP.
        # A production version could batch/asynchronously flush for higher throughput.
        self._producer.flush()
