//go:build integration

package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresProcessedEventStoreGetPendingEvents(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is not set")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	store := NewPostgresProcessedEventStore(db)

	event := RouteCalculatedEvent{
		EventID:    "evt_integration_outbox",
		EventType:  "RouteCalculated",
		ShipmentID: "shp_integration_outbox",
		Payload: RouteCalculatedPayload{
			Path:       []string{"Madrid", "Zaragoza", "Bilbao"},
			DistanceKM: 500,
		},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	_, err = db.Exec(
		`
		INSERT INTO routing.outbox_events (
			event_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3)
		`,
		event.EventID,
		event.EventType,
		payload,
	)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	defer db.Exec(
		`DELETE FROM routing.outbox_events WHERE event_id = $1`,
		event.EventID,
	)

	events, err := store.GetPendingEvents()
	if err != nil {
		t.Fatalf("GetPendingEvents failed: %v", err)
	}

	var found *RouteCalculatedEvent

	for i := range events {
		if events[i].EventID == event.EventID {
			found = &events[i]
			break
		}
	}

	if found == nil {
		t.Fatal("expected pending outbox event to be returned")
	}

	if found.EventType != event.EventType {
		t.Errorf(
			"expected event type %s, got %s",
			event.EventType,
			found.EventType,
		)
	}

	if found.ShipmentID != event.ShipmentID {
		t.Errorf(
			"expected shipment ID %s, got %s",
			event.ShipmentID,
			found.ShipmentID,
		)
	}

	if found.Payload.DistanceKM != event.Payload.DistanceKM {
		t.Errorf(
			"expected distance %.0f, got %.0f",
			event.Payload.DistanceKM,
			found.Payload.DistanceKM,
		)
	}
}