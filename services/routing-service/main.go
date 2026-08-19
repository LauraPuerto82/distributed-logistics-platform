package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
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

// processShipment contains the infrastructure-independent event processing flow:
// calculate the route, build RouteCalculated, and publish it through the injected publisher.
func processShipment(
	graph Graph,
	event ShipmentCreatedEvent,
	publisher EventPublisher,
) error {
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

	if err := publisher.PublishRouteCalculated(routeEvent); err != nil {
		return err
	}

	return nil
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

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Error reading message:", err)
			continue
		}

		var event ShipmentCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			fmt.Println("Error decoding event:", err)
			continue
		}

		// Multiple event types share the topic; Routing Service only handles ShipmentCreated.
		if event.EventType != "ShipmentCreated" {
			continue
		}

		if err := processShipment(graph, event, publisher); err != nil {
			fmt.Printf(
				"Failed to process shipment %s: %v\n",
				event.ShipmentID,
				err,
			)
			continue
		}

		fmt.Printf(
			"Route calculated and published for shipment %s\n",
			event.ShipmentID,
		)
	}
}
