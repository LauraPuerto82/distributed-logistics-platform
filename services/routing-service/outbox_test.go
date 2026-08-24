package main

import (
	"errors"
	"testing"
)

func TestPublishPendingEventsPublishesOutboxEvents(t *testing.T) {
	outboxStore := &FakeOutboxStore{
		PendingEvents: []RouteCalculatedEvent{
			{
				EventID:    "evt-outbox-1",
				EventType:  "RouteCalculated",
				ShipmentID: "shp-123",
				Payload: RouteCalculatedPayload{
					Path:       []string{"Madrid", "Zaragoza", "Bilbao"},
					DistanceKM: 640,
				},
			},
		},
	}

	publisher := &FakeEventPublisher{}

	err := publishPendingEvents(outboxStore, publisher)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(publisher.PublishedEvents) != 1 {
		t.Fatalf(
			"expected 1 published event, got %d",
			len(publisher.PublishedEvents),
		)
	}

	if publisher.PublishedEvents[0].EventID != "evt-outbox-1" {
		t.Errorf(
			"expected event ID evt-outbox-1, got %s",
			publisher.PublishedEvents[0].EventID,
		)
	}

	if len(outboxStore.PublishedIDs) != 1 {
		t.Fatalf(
			"expected 1 published marker, got %d",
			len(outboxStore.PublishedIDs),
		)
	}

	if outboxStore.PublishedIDs[0] != "evt-outbox-1" {
		t.Errorf(
			"expected event ID evt-outbox-1 to be marked published, got %s",
			outboxStore.PublishedIDs[0],
		)
	}
}

func TestPublishPendingEventsCanRepublishWhenMarkPublishedFails(t *testing.T) {
	event := RouteCalculatedEvent{
		EventID:    "evt-outbox-1",
		EventType:  "RouteCalculated",
		ShipmentID: "shp-123",
	}

	outboxStore := &FakeOutboxStore{
		PendingEvents: []RouteCalculatedEvent{event},
		MarkErr:       errors.New("database unavailable"),
	}

	publisher := &FakeEventPublisher{}

	// Kafka publication succeeds, but marking the event as published fails.
	err := publishPendingEvents(outboxStore, publisher)
	if err == nil {
		t.Fatal("expected first publishing attempt to fail")
	}

	if len(publisher.PublishedEvents) != 1 {
		t.Fatalf(
			"expected 1 published event, got %d",
			len(publisher.PublishedEvents),
		)
	}

	// Simulate PostgreSQL being available again.
	outboxStore.MarkErr = nil

	err = publishPendingEvents(outboxStore, publisher)
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}

	if len(publisher.PublishedEvents) != 2 {
		t.Fatalf(
			"expected duplicate publication, got %d published events",
			len(publisher.PublishedEvents),
		)
	}

	if publisher.PublishedEvents[0].EventID != publisher.PublishedEvents[1].EventID {
		t.Fatal("expected the retry to publish the same event ID")
	}
}
