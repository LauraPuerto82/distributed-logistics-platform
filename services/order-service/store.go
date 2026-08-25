package main

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// ShipmentStore abstracts shipment persistence from HTTP handlers.
// Production uses PostgreSQL, while tests can use in-memory storage.
type ShipmentStore interface {
	Save(shipment Shipment) error
	GetByID(id string) (Shipment, bool, error)
}

type InMemoryShipmentStore struct {
	shipments map[string]Shipment
}

type PostgresShipmentStore struct {
	conn *pgx.Conn
}

func NewInMemoryShipmentStore() *InMemoryShipmentStore {
	return &InMemoryShipmentStore{
		shipments: make(map[string]Shipment),
	}
}

func NewPostgresShipmentStore(conn *pgx.Conn) *PostgresShipmentStore {
	return &PostgresShipmentStore{
		conn: conn,
	}
}

func (s *InMemoryShipmentStore) Save(shipment Shipment) error {
	s.shipments[shipment.ID] = shipment
	return nil
}

func (s *PostgresShipmentStore) Save(shipment Shipment) error {
	query := `
		INSERT INTO shipments (
			id,
			origin,
			destination,
			weight,
			priority,
			status
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := s.conn.Exec(
		context.Background(),
		query,
		shipment.ID,
		shipment.Origin,
		shipment.Destination,
		shipment.Weight,
		shipment.Priority,
		shipment.Status,
	)

	return err
}

func (s *InMemoryShipmentStore) GetByID(id string) (Shipment, bool, error) {
	shipment, exists := s.shipments[id]
	return shipment, exists, nil
}

func (s *PostgresShipmentStore) GetByID(id string) (Shipment, bool, error) {
	query := `
		SELECT id, origin, destination, weight, priority, status
		FROM shipments
		WHERE id = $1
	`

	var shipment Shipment

	err := s.conn.QueryRow(
		context.Background(),
		query,
		id,
	).Scan(
		&shipment.ID,
		&shipment.Origin,
		&shipment.Destination,
		&shipment.Weight,
		&shipment.Priority,
		&shipment.Status,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return Shipment{}, false, nil
		}

		return Shipment{}, false, err
	}

	return shipment, true, nil
}
