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

type FakeRetryBackoff struct {
	Attempts []int
}

func (b *FakeRetryBackoff) Wait(attempt int) {
	b.Attempts = append(b.Attempts, attempt)
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

func TestHandleKafkaMessageHandlesRedeliveryAfterCommitFailure(t *testing.T) {
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
