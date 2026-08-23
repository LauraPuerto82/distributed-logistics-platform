package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

type ShipmentCreatedPayload struct {
	Origin      string  `json:"origin"`
	Destination string  `json:"destination"`
	Weight      float64 `json:"weight"`
	Priority    string  `json:"priority"`
}

type ShipmentCreatedEvent struct {
	EventID    string                 `json:"event_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  string                 `json:"timestamp"`
	ShipmentID string                 `json:"shipment_id"`
	Payload    ShipmentCreatedPayload `json:"payload"`
}

type RouteCalculatedPayload struct {
	Path       []string `json:"path"`
	DistanceKM float64  `json:"distance_km"`
}

type RouteCalculatedEvent struct {
	EventID    string                 `json:"event_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  string                 `json:"timestamp"`
	ShipmentID string                 `json:"shipment_id"`
	Payload    RouteCalculatedPayload `json:"payload"`
}

// EventPublisher decouples route processing from Kafka,
// allowing the business flow to be tested without external infrastructure.
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

type Edge struct {
	To         string
	DistanceKM float64
}

// Graph models the road network as a weighted adjacency list.
type Graph map[string][]Edge

func addUndirectedEdge(graph Graph, from string, to string, distance float64) {
	graph[from] = append(graph[from], Edge{
		To:         to,
		DistanceKM: distance,
	})

	graph[to] = append(graph[to], Edge{
		To:         from,
		DistanceKM: distance,
	})
}

// buildGraph returns a small deterministic road network for the MVP.
// Routing data can later be replaced by an external source without
// changing the shortest-path algorithm.
func buildGraph() Graph {
	graph := make(Graph)

	addUndirectedEdge(graph, "Madrid", "Zaragoza", 320)
	addUndirectedEdge(graph, "Madrid", "Valencia", 360)
	addUndirectedEdge(graph, "Zaragoza", "Logroño", 170)
	addUndirectedEdge(graph, "Logroño", "Bilbao", 150)
	addUndirectedEdge(graph, "Zaragoza", "Barcelona", 310)
	addUndirectedEdge(graph, "Barcelona", "Valencia", 350)
	addUndirectedEdge(graph, "Valencia", "Bilbao", 610)

	return graph
}

func closestUnvisitedCity(
	distances map[string]float64,
	visited map[string]bool,
) string {
	closestCity := ""
	closestDistance := math.Inf(1)

	for city, distance := range distances {
		if visited[city] {
			continue
		}

		if distance < closestDistance {
			closestCity = city
			closestDistance = distance
		}
	}

	return closestCity
}

// shortestPath calculates the minimum-distance route using Dijkstra's algorithm.
// It also reconstructs the ordered path from origin to destination.
func shortestPath(graph Graph, origin, destination string) ([]string, float64, error) {
	if _, exists := graph[origin]; !exists {
		return nil, 0, fmt.Errorf("origin city %s does not exist", origin)
	}

	if _, exists := graph[destination]; !exists {
		return nil, 0, fmt.Errorf("destination city %s does not exist", destination)
	}

	distances := make(map[string]float64)
	previous := make(map[string]string)
	visited := make(map[string]bool)

	for city := range graph {
		distances[city] = math.Inf(1)
	}

	distances[origin] = 0

	for {
		current := closestUnvisitedCity(distances, visited)

		if current == "" {
			break
		}

		if current == destination {
			break
		}

		for _, edge := range graph[current] {
			newDistance := distances[current] + edge.DistanceKM

			if newDistance < distances[edge.To] {
				// A shorter route to this city has been found.
				distances[edge.To] = newDistance
				previous[edge.To] = current
			}
		}

		visited[current] = true
	}

	if math.IsInf(distances[destination], 1) {
		return nil, 0, fmt.Errorf(
			"no route found from %s to %s",
			origin,
			destination,
		)
	}

	path := []string{}
	current := destination

	for current != "" {
		path = append(path, current)

		if current == origin {
			break
		}

		current = previous[current]
	}

	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}

	return path, distances[destination], nil
}

// publishPendingEvents publishes outbox events sequentially and marks each
// event as published only after successful Kafka delivery.
func publishPendingEvents(
	outboxStore OutboxStore,
	publisher EventPublisher,
) error {
	events, err := outboxStore.GetPendingEvents()
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := publisher.PublishRouteCalculated(event); err != nil {
			return err
		}
		if err := outboxStore.MarkPublished(event.EventID); err != nil {
			return err
		}
	}

	return nil
}

// processShipment contains the infrastructure-independent event processing flow:
// skip already processed events, calculate the route, and atomically record
// both the processed input event and the resulting RouteCalculated outbox event.
func processShipment(
	graph Graph,
	event ShipmentCreatedEvent,
	processedEventStore ProcessedEventStore,
) error {
	isProcessed, err := processedEventStore.IsProcessed(event.EventID)
	if err != nil {
		return err
	}

	if isProcessed {
		return nil
	}

	path, distance, err := shortestPath(
		graph,
		event.Payload.Origin,
		event.Payload.Destination,
	)
	if err != nil {
		return err
	}

	routeEvent := RouteCalculatedEvent{
		EventID:    "evt_" + uuid.NewString(),
		EventType:  "RouteCalculated",
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		ShipmentID: event.ShipmentID,
		Payload: RouteCalculatedPayload{
			Path:       path,
			DistanceKM: distance,
		},
	}

	if err := processedEventStore.MarkProcessedWithOutboxEvent(
		event.EventID,
		routeEvent,
	); err != nil {
		return err
	}

	return nil
}

func handleKafkaMessage(
	ctx context.Context,
	message kafka.Message,
	graph Graph,
	store ProcessedEventStore,
	committer MessageCommitter,
) error {
	var event ShipmentCreatedEvent

	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}

	// Multiple event types share the topic; Routing Service only handles ShipmentCreated.
	if event.EventType != "ShipmentCreated" {
		return committer.CommitMessages(ctx, message)
	}

	// Commit the Kafka offset only after the event has been processed successfully.
	// If processing fails, leaving the offset uncommitted allows Kafka to redeliver
	// the message; persistent event-id idempotency makes that redelivery safe.
	if err := processShipment(
		graph,
		event,
		store,
	); err != nil {
		return err
	}

	return committer.CommitMessages(ctx, message)
}

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		fmt.Println("KAFKA_BROKER is not set")
		return
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		fmt.Println("KAFKA_TOPIC is not set")
		return
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Println("DATABASE_URL is not set")
		return
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	graph := buildGraph()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: "routing-service",
	})

	defer reader.Close()

	publisher := NewKafkaEventPublisher(
		kafkaBroker,
		kafkaTopic,
	)

	defer publisher.Close()

	store := NewPostgresRoutingStore(db)

	go func() {
		for {
			if err := publishPendingEvents(store, publisher); err != nil {
				fmt.Println("Error publishing pending outbox events:", err)
			}

			time.Sleep(5 * time.Second)
		}
	}()

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("Error reading message:", err)
			continue
		}

		if err := handleKafkaMessage(
			context.Background(),
			message,
			graph,
			store,
			reader,
		); err != nil {
			fmt.Println("Error handling Kafka message:", err)
			continue
		}

		fmt.Println("Kafka message handled successfully")
	}
}
