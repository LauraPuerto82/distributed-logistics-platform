import json
from datetime import datetime, timezone

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


class DeadLetterPublisher(Protocol):
    def publish_dead_letter(
        self,
        message,
        reason: str,
    ) -> None: ...


class KafkaDeadLetterPublisher:
    def __init__(self, broker: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": broker,
            }
        )

    def publish_dead_letter(
        self,
        message,
        reason: str,
    ) -> None:
        dead_letter_event = {
            "original_topic": message.topic(),
            "original_partition": message.partition(),
            "original_offset": message.offset(),
            "reason": reason,
            "original_value": message.value().decode("utf-8", errors="replace"),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }

        self._producer.produce(
            topic=self._topic,
            key=message.key(),
            value=json.dumps(dead_letter_event),
        )

        self._producer.flush()
