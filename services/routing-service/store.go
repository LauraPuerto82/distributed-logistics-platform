package main

import (
	"database/sql"
	"encoding/json"
)

// OutboxStore abstracts pending event retrieval and publication tracking,
// keeping the outbox publishing flow independent from PostgreSQL.
type OutboxStore interface {
	GetPendingEvents() ([]RouteCalculatedEvent, error)
	MarkPublished(eventID string) error
}

// ProcessedEventStore keeps idempotency state outside the processing logic,
// so duplicate event detection can use either in-memory or persistent storage.
type ProcessedEventStore interface {
	IsProcessed(eventID string) (bool, error)
	MarkProcessedWithOutboxEvent(
		eventID string,
		outboxEvent RouteCalculatedEvent,
	) error
}

type InMemoryProcessedEventStore struct {
	processed map[string]struct{}
}

func NewInMemoryProcessedEventStore() *InMemoryProcessedEventStore {
	return &InMemoryProcessedEventStore{
		processed: make(map[string]struct{}),
	}
}

func (s *InMemoryProcessedEventStore) IsProcessed(eventID string) (bool, error) {
	_, exists := s.processed[eventID]
	return exists, nil
}

func (s *InMemoryProcessedEventStore) MarkProcessedWithOutboxEvent(
	eventID string,
	outboxEvent RouteCalculatedEvent,
) error {
	s.processed[eventID] = struct{}{}
	return nil
}

// PostgresRoutingStore persists Routing Service reliability state,
// including processed event IDs and transactional outbox events.
type PostgresRoutingStore struct {
	db *sql.DB
}

func NewPostgresRoutingStore(db *sql.DB) *PostgresRoutingStore {
	return &PostgresRoutingStore{
		db: db,
	}
}

func (s *PostgresRoutingStore) IsProcessed(eventID string) (bool, error) {
	var exists bool

	err := s.db.QueryRow(
		`
        SELECT EXISTS (
            SELECT 1
            FROM routing.processed_events
            WHERE event_id = $1
        )
        `,
		eventID,
	).Scan(&exists)

	if err != nil {
		return false, err
	}

	return exists, nil
}

func (s *PostgresRoutingStore) MarkProcessedWithOutboxEvent(
	eventID string,
	outboxEvent RouteCalculatedEvent,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`
		INSERT INTO routing.processed_events (event_id)
		VALUES ($1)
		`,
		eventID,
	); err != nil {
		return err
	}

	payload, err := json.Marshal(outboxEvent)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(
		`
		INSERT INTO routing.outbox_events (
			event_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3)
		`,
		outboxEvent.EventID,
		outboxEvent.EventType,
		payload,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *PostgresRoutingStore) GetPendingEvents() ([]RouteCalculatedEvent, error) {
	rows, err := s.db.Query(`
		SELECT payload
		FROM routing.outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []RouteCalculatedEvent

	for rows.Next() {
		var payload []byte

		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}

		var event RouteCalculatedEvent

		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *PostgresRoutingStore) MarkPublished(eventID string) error {
	_, err := s.db.Exec(
		`
		UPDATE routing.outbox_events
		SET published_at = NOW()
		WHERE event_id = $1
		`,
		eventID,
	)

	return err
}
