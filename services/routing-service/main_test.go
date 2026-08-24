package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
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
	markErrors   []error
	markCalls    int
}

func (s *FakeProcessedEventStore) IsProcessed(eventID string) (bool, error) {
	return s.processed, nil
}

func (s *FakeProcessedEventStore) MarkProcessedWithOutboxEvent(
	eventID string,
	outboxEvent RouteCalculatedEvent,
) error {
	s.markCalls++

	if len(s.markErrors) > 0 {
		err := s.markErrors[0]
		s.markErrors = s.markErrors[1:]

		if err != nil {
			return err
		}
	}

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

type FakeMessageCommitter struct {
	CommittedMessages []kafka.Message
	CommitErr         error
}

func (c *FakeMessageCommitter) CommitMessages(
	ctx context.Context,
	msgs ...kafka.Message,
) error {
	if c.CommitErr != nil {
		return c.CommitErr
	}

	c.CommittedMessages = append(c.CommittedMessages, msgs...)
	return nil
}

type FakeDeadLetterPublisher struct {
	PublishedMessages []kafka.Message
	Reasons           []string
	Err               error
}

func (p *FakeDeadLetterPublisher) PublishDeadLetter(
	ctx context.Context,
	message kafka.Message,
	reason string,
) error {
	if p.Err != nil {
		return p.Err
	}

	p.PublishedMessages = append(p.PublishedMessages, message)
	p.Reasons = append(p.Reasons, reason)
	return nil
}

type FakeRetryBackoff struct {
	Attempts []int
}

func (b *FakeRetryBackoff) Wait(attempt int) {
	b.Attempts = append(b.Attempts, attempt)
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

func TestHandleKafkaMessageCommitsIgnoredEvent(t *testing.T) {
	graph := buildGraph()
	store := NewInMemoryProcessedEventStore()
	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}

	event := RouteCalculatedEvent{
		EventID:    "evt_route_123",
		EventType:  "RouteCalculated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_123",
		Payload: RouteCalculatedPayload{
			Path:       []string{"Madrid", "Zaragoza"},
			DistanceKM: 320,
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    25,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)
	if err != nil {
		t.Fatalf("expected ignored event to be handled successfully, got %v", err)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if committer.CommittedMessages[0].Offset != message.Offset {
		t.Fatalf(
			"expected committed offset %d, got %d",
			message.Offset,
			committer.CommittedMessages[0].Offset,
		)
	}
}

func TestHandleKafkaMessageCommitsSuccessfullyProcessedShipment(t *testing.T) {
	graph := buildGraph()
	store := NewInMemoryProcessedEventStore()
	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_shipment_123",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_123",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    26,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)
	if err != nil {
		t.Fatalf("expected message to be handled successfully, got %v", err)
	}

	isProcessed, err := store.IsProcessed(event.EventID)
	if err != nil {
		t.Fatalf("failed to check processed event: %v", err)
	}

	if !isProcessed {
		t.Fatal("expected shipment event to be marked as processed")
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if committer.CommittedMessages[0].Offset != message.Offset {
		t.Fatalf(
			"expected committed offset %d, got %d",
			message.Offset,
			committer.CommittedMessages[0].Offset,
		)
	}
}

type FailingProcessedEventStore struct {
	ProcessErr error
}

func (s *FailingProcessedEventStore) IsProcessed(eventID string) (bool, error) {
	return false, nil
}

func (s *FailingProcessedEventStore) MarkProcessedWithOutboxEvent(
	eventID string,
	outboxEvent RouteCalculatedEvent,
) error {
	return s.ProcessErr
}

func TestHandleKafkaMessageDoesNotCommitWhenRetriesExhaustedAndDeadLetterFails(t *testing.T) {
	graph := buildGraph()

	store := &FailingProcessedEventStore{
		ProcessErr: errors.New("database unavailable"),
	}

	committer := &FakeMessageCommitter{}

	deadLetterPublisher := &FakeDeadLetterPublisher{
		Err: errors.New("dlq unavailable"),
	}

	event := ShipmentCreatedEvent{
		EventID:    "evt_shipment_456",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_456",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    27,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err == nil {
		t.Fatal("expected message handling to fail")
	}

	if len(deadLetterPublisher.PublishedMessages) != 0 {
		t.Fatalf(
			"expected no successful dead-letter publications, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 0 {
		t.Fatalf(
			"expected no committed messages, got %d",
			len(committer.CommittedMessages),
		)
	}
}

func TestHandleKafkaMessageCanRetryCommitAfterProcessingSucceeds(t *testing.T) {
	graph := buildGraph()
	store := &FakeProcessedEventStore{}

	committer := &FakeMessageCommitter{
		CommitErr: errors.New("commit unavailable"),
	}
	deadLetterPublisher := &FakeDeadLetterPublisher{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_shipment_789",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_789",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    28,
		Value:     value,
	}

	// Processing succeeds, but committing the Kafka offset fails.
	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err == nil {
		t.Fatal("expected commit failure")
	}

	isProcessed, err := store.IsProcessed(event.EventID)
	if err != nil {
		t.Fatalf("failed to check processed event: %v", err)
	}

	if !isProcessed {
		t.Fatal("expected event to remain processed after commit failure")
	}

	// Simulate Kafka redelivering the same message later.
	committer.CommitErr = nil

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)
	if err != nil {
		t.Fatalf("expected redelivery to succeed, got %v", err)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 successful committed message, got %d",
			len(committer.CommittedMessages),
		)
	}
	if len(store.outboxEvents) != 1 {
		t.Fatalf(
			"expected 1 outbox event after redelivery, got %d",
			len(store.outboxEvents),
		)
	}
}

func TestHandleKafkaMessageSendsMalformedJSONToDeadLetterAndCommits(t *testing.T) {
	graph := buildGraph()
	store := NewInMemoryProcessedEventStore()
	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    29,
		Value:     []byte(`{"event_id": "evt_invalid_001", invalid json}`),
	}

	err := handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err != nil {
		t.Fatalf("expected malformed message to be handled successfully, got %v", err)
	}

	if len(deadLetterPublisher.PublishedMessages) != 1 {
		t.Fatalf(
			"expected 1 dead-letter message, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(deadLetterPublisher.Reasons) != 1 {
		t.Fatalf(
			"expected 1 dead-letter reason, got %d",
			len(deadLetterPublisher.Reasons),
		)
	}

	if deadLetterPublisher.Reasons[0] != "invalid JSON" {
		t.Fatalf(
			"expected dead-letter reason %q, got %q",
			"invalid JSON",
			deadLetterPublisher.Reasons[0],
		)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if committer.CommittedMessages[0].Offset != message.Offset {
		t.Fatalf(
			"expected committed offset %d, got %d",
			message.Offset,
			committer.CommittedMessages[0].Offset,
		)
	}
}

func TestHandleKafkaMessageDoesNotCommitWhenDeadLetterPublicationFails(t *testing.T) {
	graph := buildGraph()
	store := NewInMemoryProcessedEventStore()
	committer := &FakeMessageCommitter{}

	deadLetterPublisher := &FakeDeadLetterPublisher{
		Err: errors.New("dlq unavailable"),
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    30,
		Value:     []byte(`{"event_id": "evt_invalid_002", invalid json}`),
	}

	err := handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err == nil {
		t.Fatal("expected dead-letter publication to fail")
	}

	if len(deadLetterPublisher.PublishedMessages) != 0 {
		t.Fatalf(
			"expected no successful dead-letter publications, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 0 {
		t.Fatalf(
			"expected original message not to be committed, got %d commits",
			len(committer.CommittedMessages),
		)
	}
}

func TestHandleKafkaMessageCanRepublishDeadLetterAfterCommitFailure(t *testing.T) {
	graph := buildGraph()
	store := NewInMemoryProcessedEventStore()

	committer := &FakeMessageCommitter{
		CommitErr: errors.New("commit unavailable"),
	}

	deadLetterPublisher := &FakeDeadLetterPublisher{}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    31,
		Value:     []byte(`{"event_id": "evt_invalid_003", invalid json}`),
	}

	// First delivery: DLQ publication succeeds, but offset commit fails.
	err := handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err == nil {
		t.Fatal("expected commit failure")
	}

	if len(deadLetterPublisher.PublishedMessages) != 1 {
		t.Fatalf(
			"expected 1 dead-letter publication, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 0 {
		t.Fatalf(
			"expected no successful commits, got %d",
			len(committer.CommittedMessages),
		)
	}

	// Simulate Kafka redelivering the same original message.
	committer.CommitErr = nil

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		ExponentialBackoff{},
	)

	if err != nil {
		t.Fatalf("expected redelivery to succeed, got %v", err)
	}

	if len(deadLetterPublisher.PublishedMessages) != 2 {
		t.Fatalf(
			"expected duplicate dead-letter publication after redelivery, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 successful commit, got %d",
			len(committer.CommittedMessages),
		)
	}
}

func TestHandleKafkaMessageRetriesTransientProcessingFailure(t *testing.T) {
	graph := buildGraph()

	store := &FakeProcessedEventStore{
		markErrors: []error{
			errors.New("database temporarily unavailable"),
			nil,
		},
	}

	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}
	backoff := &FakeRetryBackoff{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_retry_001",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_retry_001",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    32,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		backoff,
	)

	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}

	if store.markCalls != 2 {
		t.Fatalf(
			"expected 2 processing attempts, got %d",
			store.markCalls,
		)
	}

	if len(store.outboxEvents) != 1 {
		t.Fatalf(
			"expected 1 outbox event, got %d",
			len(store.outboxEvents),
		)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if len(deadLetterPublisher.PublishedMessages) != 0 {
		t.Fatalf(
			"expected no dead-letter messages, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if !reflect.DeepEqual(backoff.Attempts, []int{1}) {
		t.Fatalf(
			"expected backoff after attempt 1, got %v",
			backoff.Attempts,
		)
	}
}

func TestHandleKafkaMessageSendsToDeadLetterAfterRetriesExhausted(t *testing.T) {
	graph := buildGraph()

	store := &FakeProcessedEventStore{
		markErrors: []error{
			errors.New("database temporarily unavailable"),
			errors.New("database temporarily unavailable"),
			errors.New("database temporarily unavailable"),
		},
	}

	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}
	backoff := &FakeRetryBackoff{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_retry_exhausted_001",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_retry_exhausted_001",
		Payload: ShipmentCreatedPayload{
			Origin:      "Madrid",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    33,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		backoff,
	)

	if err != nil {
		t.Fatalf("expected exhausted retries to be handled through DLQ, got %v", err)
	}

	if store.markCalls != 3 {
		t.Fatalf(
			"expected 3 processing attempts, got %d",
			store.markCalls,
		)
	}

	if len(deadLetterPublisher.PublishedMessages) != 1 {
		t.Fatalf(
			"expected 1 dead-letter publication, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if len(store.outboxEvents) != 0 {
		t.Fatalf(
			"expected no outbox events after exhausted retries, got %d",
			len(store.outboxEvents),
		)
	}

	if !reflect.DeepEqual(backoff.Attempts, []int{1, 2}) {
		t.Fatalf(
			"expected backoff after attempts 1 and 2, got %v",
			backoff.Attempts,
		)
	}

	if len(deadLetterPublisher.Reasons) != 1 {
		t.Fatalf(
			"expected 1 dead-letter reason, got %d",
			len(deadLetterPublisher.Reasons),
		)
	}

	if deadLetterPublisher.Reasons[0] != "processing retries exhausted" {
		t.Fatalf(
			"expected retries exhausted reason, got %q",
			deadLetterPublisher.Reasons[0],
		)
	}
}

func TestHandleKafkaMessageSendsPermanentProcessingErrorToDeadLetter(t *testing.T) {
	graph := buildGraph()
	store := &FakeProcessedEventStore{}
	committer := &FakeMessageCommitter{}
	deadLetterPublisher := &FakeDeadLetterPublisher{}
	backoff := &FakeRetryBackoff{}

	event := ShipmentCreatedEvent{
		EventID:    "evt_permanent_001",
		EventType:  "ShipmentCreated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: "shp_permanent_001",
		Payload: ShipmentCreatedPayload{
			Origin:      "Atlantis",
			Destination: "Bilbao",
			Weight:      15,
			Priority:    "HIGH",
		},
	}

	value, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	message := kafka.Message{
		Topic:     "shipment-events",
		Partition: 0,
		Offset:    34,
		Value:     value,
	}

	err = handleKafkaMessage(
		context.Background(),
		message,
		graph,
		store,
		committer,
		deadLetterPublisher,
		backoff,
	)

	if err != nil {
		t.Fatalf("expected permanent error to be handled through DLQ, got %v", err)
	}

	if len(deadLetterPublisher.PublishedMessages) != 1 {
		t.Fatalf(
			"expected 1 dead-letter publication, got %d",
			len(deadLetterPublisher.PublishedMessages),
		)
	}

	if len(committer.CommittedMessages) != 1 {
		t.Fatalf(
			"expected 1 committed message, got %d",
			len(committer.CommittedMessages),
		)
	}

	if len(deadLetterPublisher.Reasons) != 1 {
		t.Fatalf(
			"expected 1 dead-letter reason, got %d",
			len(deadLetterPublisher.Reasons),
		)
	}

	if deadLetterPublisher.Reasons[0] != "permanent processing failure" {
		t.Fatalf(
			"expected permanent processing failure reason, got %q",
			deadLetterPublisher.Reasons[0],
		)
	}

	if len(backoff.Attempts) != 0 {
		t.Fatalf(
			"expected no retry backoff for permanent error, got %v",
			backoff.Attempts,
		)
	}
}

func TestShouldRetryReturnsFalseForPermanentProcessingError(t *testing.T) {
	err := &PermanentProcessingError{
		Err: errors.New("origin city Atlantis does not exist"),
	}

	if shouldRetry(err) {
		t.Fatal("expected permanent processing error not to be retryable")
	}
}

func TestShouldRetryReturnsTrueForTransientError(t *testing.T) {
	err := errors.New("database temporarily unavailable")

	if !shouldRetry(err) {
		t.Fatal("expected transient error to be retryable")
	}
}
