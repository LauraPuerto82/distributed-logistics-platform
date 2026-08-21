package main

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// FakeEventPublisher captures published events and can simulate
// infrastructure failures without requiring a running Kafka broker.
type FakeEventPublisher struct {
	PublishedEvents []RouteCalculatedEvent
	Err             error
}

func (p *FakeEventPublisher) PublishRouteCalculated(event RouteCalculatedEvent) error {
	if p.Err != nil {
		return p.Err
	}

	p.PublishedEvents = append(p.PublishedEvents, event)
	return nil
}

type FakeProcessedEventStore struct {
	processed    bool
	outboxEvents []RouteCalculatedEvent
	markErr      error
}

func (s *FakeProcessedEventStore) IsProcessed(eventID string) (bool, error) {
	return s.processed, nil
}

func (s *FakeProcessedEventStore) MarkProcessedWithOutboxEvent(
	eventID string,
	outboxEvent RouteCalculatedEvent,
) error {
	if s.markErr != nil {
		return s.markErr
	}

	s.processed = true
	s.outboxEvents = append(s.outboxEvents, outboxEvent)
	return nil
}

type FakeOutboxStore struct {
	PendingEvents []RouteCalculatedEvent
	PublishedIDs  []string
	MarkErr       error
}

func (s *FakeOutboxStore) GetPendingEvents() ([]RouteCalculatedEvent, error) {
	return s.PendingEvents, nil
}

func (s *FakeOutboxStore) MarkPublished(eventID string) error {
	if s.MarkErr != nil {
		return s.MarkErr
	}

	s.PublishedIDs = append(s.PublishedIDs, eventID)
	return nil
}

func TestShortestPathMadridToBilbao(t *testing.T) {
	graph := buildGraph()

	path, distance, err := shortestPath(graph, "Madrid", "Bilbao")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedPath := []string{
		"Madrid",
		"Zaragoza",
		"Logroño",
		"Bilbao",
	}

	if !reflect.DeepEqual(path, expectedPath) {
		t.Errorf("expected path %v, got %v", expectedPath, path)
	}

	expectedDistance := 640.0

	if distance != expectedDistance {
		t.Errorf("expected distance %.0f, got %.0f", expectedDistance, distance)
	}
}

func TestShortestPathSameOriginAndDestination(t *testing.T) {
	graph := buildGraph()

	path, distance, err := shortestPath(graph, "Madrid", "Madrid")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expectedPath := []string{"Madrid"}

	if !reflect.DeepEqual(path, expectedPath) {
		t.Errorf("expected path %v, got %v", expectedPath, path)
	}

	if distance != 0 {
		t.Errorf("expected distance 0, got %.0f", distance)
	}
}

func TestShortestPathOriginDoesNotExist(t *testing.T) {
	graph := buildGraph()

	_, _, err := shortestPath(graph, "Atlantis", "Bilbao")

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestShortestPathDestinationDoesNotExist(t *testing.T) {
	graph := buildGraph()

	_, _, err := shortestPath(graph, "Madrid", "Atlantis")

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestShortestPathNoRouteExists(t *testing.T) {
	graph := make(Graph)

	addUndirectedEdge(graph, "Madrid", "Zaragoza", 320)

	// Bilbao exists in the graph, but it is disconnected.
	graph["Bilbao"] = []Edge{}

	_, _, err := shortestPath(graph, "Madrid", "Bilbao")

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestProcessShipmentStoresRouteCalculatedOutboxEvent(t *testing.T) {
	graph := buildGraph()
	processedEventStore := &FakeProcessedEventStore{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_input",
		EventType:  "ShipmentCreated",
		ShipmentID: "shp_test_1",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	err := processShipment(
		graph,
		event,
		processedEventStore,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(processedEventStore.outboxEvents) != 1 {
		t.Fatalf(
			"expected 1 outbox event, got %d",
			len(processedEventStore.outboxEvents),
		)
	}

	routeEvent := processedEventStore.outboxEvents[0]

	if routeEvent.EventType != "RouteCalculated" {
		t.Errorf(
			"expected event type RouteCalculated, got %s",
			routeEvent.EventType,
		)
	}

	if routeEvent.EventID == "" {
		t.Errorf("expected event ID to be generated")
	}

	if routeEvent.ShipmentID != event.ShipmentID {
		t.Errorf(
			"expected shipment ID %s, got %s",
			event.ShipmentID,
			routeEvent.ShipmentID,
		)
	}

	expectedPath := []string{
		"Madrid",
		"Zaragoza",
		"Logroño",
		"Bilbao",
	}

	if !reflect.DeepEqual(routeEvent.Payload.Path, expectedPath) {
		t.Errorf(
			"expected path %v, got %v",
			expectedPath,
			routeEvent.Payload.Path,
		)
	}

	if routeEvent.Payload.DistanceKM != 640 {
		t.Errorf(
			"expected distance 640, got %.0f",
			routeEvent.Payload.DistanceKM,
		)
	}
}

func TestProcessShipmentReturnsErrorWhenStoreFails(t *testing.T) {
	graph := buildGraph()

	processedEventStore := &FakeProcessedEventStore{
		markErr: errors.New("database unavailable"),
	}

	event := ShipmentCreatedEvent{
		EventID:    "evt_input",
		EventType:  "ShipmentCreated",
		ShipmentID: "shp_test_1",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	err := processShipment(
		graph,
		event,
		processedEventStore,
	)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestProcessShipmentDoesNotStoreDuplicateEvent(t *testing.T) {
	graph := buildGraph()
	processedEventStore := &FakeProcessedEventStore{}

	event := ShipmentCreatedEvent{
		EventID:    "evt-123",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp-123",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	if err := processShipment(graph, event, processedEventStore); err != nil {
		t.Fatalf("first processing failed: %v", err)
	}

	if err := processShipment(graph, event, processedEventStore); err != nil {
		t.Fatalf("second processing failed: %v", err)
	}

	if len(processedEventStore.outboxEvents) != 1 {
		t.Fatalf(
			"expected 1 outbox event, got %d",
			len(processedEventStore.outboxEvents),
		)
	}
}

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
