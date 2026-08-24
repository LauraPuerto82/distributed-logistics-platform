package main

import (
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

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
		return nil, 0, &PermanentProcessingError{
			Err: fmt.Errorf("origin city %s does not exist", origin),
		}
	}

	if _, exists := graph[destination]; !exists {
		return nil, 0, &PermanentProcessingError{
			Err: fmt.Errorf("destination city %s does not exist", destination),
		}
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
		return nil, 0, &PermanentProcessingError{
			Err: fmt.Errorf(
				"no route found from %s to %s",
				origin,
				destination,
			),
		}
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
