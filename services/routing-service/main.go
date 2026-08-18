package main

import (
	"fmt"
	"math"
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

type Edge struct {
	To         string
	DistanceKM float64
}

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

func main() {
	fmt.Println("Routing Service starting...")

	graph := buildGraph()

	path, distance, err := shortestPath(graph, "Madrid", "Bilbao")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Path:", path)
	fmt.Println("Distance:", distance)
}