package main

// publishPendingEvents publishes outbox events sequentially and marks each
// event as published only after successful Kafka delivery.
func publishPendingEvents(
	outboxStore OutboxStore,
	publisher EventPublisher,
) error {
	events, err := outboxStore.GetPendingEvents()
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := publisher.PublishRouteCalculated(event); err != nil {
			return err
		}
		if err := outboxStore.MarkPublished(event.EventID); err != nil {
			return err
		}
	}

	return nil
}
