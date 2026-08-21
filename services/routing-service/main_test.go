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

func TestProcessShipmentPublishesRouteCalculated(t *testing.T) {
	graph := buildGraph()
	publisher := &FakeEventPublisher{}
	processedEventStore := NewInMemoryProcessedEventStore()

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
		publisher,
		processedEventStore,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(publisher.PublishedEvents) != 1 {
		t.Fatalf(
			"expected 1 published event, got %d",
			len(publisher.PublishedEvents),
		)
	}

	routeEvent := publisher.PublishedEvents[0]

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

func TestProcessShipmentReturnsErrorWhenPublisherFails(t *testing.T) {
	graph := buildGraph()

	publisher := &FakeEventPublisher{
		Err: errors.New("kafka unavailable"),
	}

	processedEventStore := NewInMemoryProcessedEventStore()

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
		publisher,
		processedEventStore,
	)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestProcessShipmentDoesNotPublishDuplicateEvent(t *testing.T) {
	graph := buildGraph()
	publisher := &FakeEventPublisher{}
	processedEventStore := NewInMemoryProcessedEventStore()

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

	if err := processShipment(graph, event, publisher, processedEventStore); err != nil {
		t.Fatalf("first processing failed: %v", err)
	}

	if err := processShipment(graph, event, publisher, processedEventStore); err != nil {
		t.Fatalf("second processing failed: %v", err)
	}

	if len(publisher.PublishedEvents) != 1 {
		t.Fatalf(
			"expected 1 published event, got %d",
			len(publisher.PublishedEvents),
		)
	}
}
