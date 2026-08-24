package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// EventPublisher abstracts publication of RouteCalculated events,
// keeping the outbox publishing flow independent from Kafka.
type EventPublisher interface {
	PublishRouteCalculated(event RouteCalculatedEvent) error
}

type KafkaEventPublisher struct {
	writer *kafka.Writer
}

func NewKafkaEventPublisher(brokerAddress string, topic string) *KafkaEventPublisher {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokerAddress),
		Topic: topic,
	}

	return &KafkaEventPublisher{
		writer: writer,
	}
}

func (p *KafkaEventPublisher) PublishRouteCalculated(event RouteCalculatedEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	message := kafka.Message{
		// Using shipment_id as the key preserves per-shipment ordering
		// when the topic is partitioned.
		Key:   []byte(event.ShipmentID),
		Value: value,
	}

	return p.writer.WriteMessages(
		context.Background(),
		message,
	)
}

func (p *KafkaEventPublisher) Close() error {
	return p.writer.Close()
}

// MessageCommitter abstracts Kafka offset commits so message-processing
// semantics can be tested without a running Kafka broker.
type MessageCommitter interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// DeadLetterPublisher abstracts publication of messages that cannot be
// processed successfully and should leave the normal consumer flow.
type DeadLetterPublisher interface {
	PublishDeadLetter(
		ctx context.Context,
		message kafka.Message,
		reason string,
	) error
}

type DeadLetterEvent struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition int    `json:"original_partition"`
	OriginalOffset    int64  `json:"original_offset"`
	Reason            string `json:"reason"`
	FailedAt          string `json:"failed_at"`
	OriginalValue     string `json:"original_value"`
}

type KafkaDeadLetterPublisher struct {
	writer *kafka.Writer
}

func NewKafkaDeadLetterPublisher(
	brokerAddress string,
	topic string,
) *KafkaDeadLetterPublisher {
	return &KafkaDeadLetterPublisher{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokerAddress),
			Topic: topic,
		},
	}
}

func (p *KafkaDeadLetterPublisher) PublishDeadLetter(
	ctx context.Context,
	message kafka.Message,
	reason string,
) error {
	deadLetterEvent := DeadLetterEvent{
		OriginalTopic:     message.Topic,
		OriginalPartition: message.Partition,
		OriginalOffset:    message.Offset,
		Reason:            reason,
		FailedAt:          time.Now().UTC().Format(time.RFC3339),
		OriginalValue:     string(message.Value),
	}

	value, err := json.Marshal(deadLetterEvent)
	if err != nil {
		return fmt.Errorf("marshal dead-letter event: %w", err)
	}

	return p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   message.Key,
			Value: value,
		},
	)
}

func (p *KafkaDeadLetterPublisher) Close() error {
	return p.writer.Close()
}
