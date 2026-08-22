from prediction_service.publisher import EventPublisher
from prediction_service.store import OutboxStore


def publish_pending_events(
    outbox_store: OutboxStore,
    publisher: EventPublisher,
) -> None:
    events = outbox_store.get_pending_events()

    for event in events:
        # Mark the event only after successful publication. If marking fails afterwards,
        # the event remains pending and may be published again (at-least-once delivery).
        publisher.publish_eta_predicted(event)
        outbox_store.mark_published(event.event_id)
