package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

type Shipment struct {
	ID          string  `json:"id"`
	Origin      string  `json:"origin" binding:"required"`
	Destination string  `json:"destination" binding:"required"`
	Weight      float64 `json:"weight" binding:"required,gt=0"`
	Priority    string  `json:"priority" binding:"required,oneof=LOW MEDIUM HIGH"`
	Status      string  `json:"status"`
}

type ShipmentCreatedPayload struct {
	Origin      string  `json:"origin"`
	Destination string  `json:"destination"`
	Weight      float64 `json:"weight"`
	Priority    string  `json:"priority"`
}

// ShipmentCreatedEvent is the external event contract published by Order Service.
// It is kept separate from the internal Shipment representation.
type ShipmentCreatedEvent struct {
	EventID    string                 `json:"event_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  string                 `json:"timestamp"`
	ShipmentID string                 `json:"shipment_id"`
	Payload    ShipmentCreatedPayload `json:"payload"`
}

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

// EventPublisher keeps event delivery infrastructure outside the HTTP handlers.
// Production publishes to Kafka; tests can inject a controlled test implementation.
type EventPublisher interface {
	PublishShipmentCreated(event ShipmentCreatedEvent) error
}

// NoOpEventPublisher is used when event publication should have no side effects.
type NoOpEventPublisher struct {
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

func (p *NoOpEventPublisher) PublishShipmentCreated(event ShipmentCreatedEvent) error {
	return nil
}

func (p *KafkaEventPublisher) PublishShipmentCreated(event ShipmentCreatedEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Use shipment_id as the message key so events for the same shipment
	// can be routed to the same partition and preserve their relative order.
	message := kafka.Message{
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

func setupRouter(
	store ShipmentStore,
	publisher EventPublisher,
) *gin.Engine {
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	router.POST("/shipments", func(c *gin.Context) {
		var shipment Shipment

		if err := c.ShouldBindJSON(&shipment); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request body",
			})
			return
		}

		if shipment.Origin == shipment.Destination {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "origin and destination must be different",
			})
			return
		}

		shipment.ID = "shp_" + uuid.NewString()
		shipment.Status = "CREATED"

		// Persist before publishing the event.
		// This is intentionally non-atomic for the current MVP; see ADR-008.
		if err := store.Save(shipment); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to save shipment",
			})
			return
		}

		event := ShipmentCreatedEvent{
			EventID:    "evt_" + uuid.NewString(),
			EventType:  "ShipmentCreated",
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			ShipmentID: shipment.ID,
			Payload: ShipmentCreatedPayload{
				Origin:      shipment.Origin,
				Destination: shipment.Destination,
				Weight:      shipment.Weight,
				Priority:    shipment.Priority,
			},
		}

		if err := publisher.PublishShipmentCreated(event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to publish shipment created event",
			})
			return
		}

		c.JSON(http.StatusCreated, shipment)
	})

	router.GET("/shipments/:id", func(c *gin.Context) {
		id := c.Param("id")

		shipment, exists, err := store.GetByID(id)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to get shipment",
			})
			return
		}

		if !exists {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "shipment not found",
			})
			return
		}

		c.JSON(http.StatusOK, shipment)
	})

	return router
}

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is not set")
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_TOPIC is not set")
	}

	conn, err := pgx.Connect(ctx, databaseURL)

	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL:", err)
	}

	defer conn.Close(ctx)

	if err := conn.Ping(ctx); err != nil {
		log.Fatal("PostgreSQL ping failed:", err)
	}

	log.Println("Connected to PostgreSQL")
	
	store := NewPostgresShipmentStore(conn)
	publisher := NewKafkaEventPublisher(kafkaBroker, kafkaTopic)
	defer publisher.Close()

	router := setupRouter(store, publisher)
	router.Run(":8080")
}
