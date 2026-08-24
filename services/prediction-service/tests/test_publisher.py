import json

from prediction_service.publisher import KafkaDeadLetterPublisher


class FakeKafkaMessage:
    def topic(self):
        return "shipment-events"

    def partition(self):
        return 2

    def offset(self):
        return 42

    def key(self):
        return b"shp_001"

    def value(self):
        return b'{"event_type":"RouteCalculated"}'


class FakeProducer:
    def __init__(self):
        self.produced = []
        self.flush_calls = 0

    def produce(self, **kwargs):
        self.produced.append(kwargs)

    def flush(self):
        self.flush_calls += 1


def test_kafka_dead_letter_publisher_preserves_failure_context():
    publisher = KafkaDeadLetterPublisher(
        broker="unused",
        topic="prediction-service-dlq",
    )

    fake_producer = FakeProducer()
    publisher._producer = fake_producer

    message = FakeKafkaMessage()

    publisher.publish_dead_letter(
        message,
        "processing retries exhausted",
    )

    assert len(fake_producer.produced) == 1

    produced = fake_producer.produced[0]

    assert produced["topic"] == "prediction-service-dlq"
    assert produced["key"] == b"shp_001"

    payload = json.loads(produced["value"])

    assert payload["original_topic"] == "shipment-events"
    assert payload["original_partition"] == 2
    assert payload["original_offset"] == 42
    assert payload["reason"] == "processing retries exhausted"
    assert payload["original_value"] == '{"event_type":"RouteCalculated"}'
    assert "failed_at" in payload

    assert fake_producer.flush_calls == 1