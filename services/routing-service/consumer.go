package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const maxProcessingAttempts = 3

type RetryBackoff interface {
	Wait(attempt int)
}

type ExponentialBackoff struct{}

func (ExponentialBackoff) Wait(attempt int) {
	delay := time.Second * time.Duration(1<<(attempt-1))
	time.Sleep(delay)
}

type PermanentProcessingError struct {
	Err error
}

func (e *PermanentProcessingError) Error() string {
	return e.Err.Error()
}

func (e *PermanentProcessingError) Unwrap() error {
	return e.Err
}

func shouldRetry(err error) bool {
	var permanentErr *PermanentProcessingError
	return !errors.As(err, &permanentErr)
}

func processShipmentWithRetry(
	graph Graph,
	event ShipmentCreatedEvent,
	store ProcessedEventStore,
	maxAttempts int,
	backoff RetryBackoff,
) error {
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := processShipment(
			graph,
			event,
			store,
		); err != nil {
			if !shouldRetry(err) {
				return err
			}

			if attempt < maxAttempts {
				backoff.Wait(attempt)
			}

			lastErr = err
			continue
		}

		return nil
	}

	return lastErr
}

// handleKafkaMessage processes a single Kafka message.
// Transient failures use bounded retries with exponential backoff.
// Permanent failures skip retries. Permanent failures and exhausted retries
// are published to the DLQ before the original Kafka offset is committed.
func handleKafkaMessage(
	ctx context.Context,
	message kafka.Message,
	graph Graph,
	store ProcessedEventStore,
	committer MessageCommitter,
	deadLetterPublisher DeadLetterPublisher,
	backoff RetryBackoff,
) error {
	var event ShipmentCreatedEvent

	if err := json.Unmarshal(message.Value, &event); err != nil {
		if err := deadLetterPublisher.PublishDeadLetter(
			ctx,
			message,
			"invalid JSON",
		); err != nil {
			return fmt.Errorf("publish dead-letter message: %w", err)
		}

		return committer.CommitMessages(ctx, message)
	}

	// Multiple event types share the topic; Routing Service only handles ShipmentCreated.
	if event.EventType != "ShipmentCreated" {
		return committer.CommitMessages(ctx, message)
	}

	// Processing must reach a terminal outcome before the Kafka offset is committed:
	// either successful processing or successful publication to the DLQ.
	// If DLQ publication fails, the offset remains uncommitted so Kafka can redeliver.
	if err := processShipmentWithRetry(
		graph,
		event,
		store,
		maxProcessingAttempts,
		backoff,
	); err != nil {
		reason := "processing retries exhausted"

		var permanentErr *PermanentProcessingError
		if errors.As(err, &permanentErr) {
			reason = "permanent processing failure"
		}

		if dlqErr := deadLetterPublisher.PublishDeadLetter(
			ctx,
			message,
			reason,
		); dlqErr != nil {
			return fmt.Errorf("publish dead-letter message: %w", dlqErr)
		}

		return committer.CommitMessages(ctx, message)
	}

	return committer.CommitMessages(ctx, message)
}
