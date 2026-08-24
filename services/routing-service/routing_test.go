package main

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

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

func TestShortestPathNoRouteReturnsPermanentProcessingError(t *testing.T) {
	graph := make(Graph)

	addUndirectedEdge(graph, "Madrid", "Zaragoza", 320)
	graph["Bilbao"] = []Edge{}

	_, _, err := shortestPath(graph, "Madrid", "Bilbao")

	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	var permanentErr *PermanentProcessingError

	if !errors.As(err, &permanentErr) {
		t.Fatalf(
			"expected PermanentProcessingError, got %T",
			err,
		)
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
