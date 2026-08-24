package main

import (
	"context"
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
