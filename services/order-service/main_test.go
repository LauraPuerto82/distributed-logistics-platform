package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthEndpoint(t *testing.T) {
	store := NewInMemoryShipmentStore()
    router := setupRouter(store)

	response := httptest.NewRecorder()

	request, _ := http.NewRequest(
		http.MethodGet,
		"/health",
		nil,
	)

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	var body map[string]string

	err := json.Unmarshal(response.Body.Bytes(), &body)
	if err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	if body["status"] != "ok" {
		t.Errorf(
			"expected status body to be ok, got %s",
			body["status"],
		)
	}
}

func TestCreateShipment(t *testing.T) {
	store := NewInMemoryShipmentStore()
    router := setupRouter(store)

	requestBody := `{
		"origin": "Madrid",
		"destination": "Barcelona",
		"weight": 15,
		"priority": "HIGH"
	}`

	request, _ := http.NewRequest(
		http.MethodPost,
		"/shipments",
		strings.NewReader(requestBody),
	)

	request.Header.Set("Content-Type", "application/json")

	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusCreated,
			response.Code,
		)
	}

	var shipment Shipment

	err := json.Unmarshal(response.Body.Bytes(), &shipment)
	if err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	if shipment.Origin != "Madrid" {
		t.Errorf("expected origin Madrid, got %s", shipment.Origin)
	}

	if shipment.Destination != "Barcelona" {
		t.Errorf("expected destination Barcelona, got %s", shipment.Destination)
	}

	if shipment.Weight != 15 {
		t.Errorf("expected weight 15, got %f", shipment.Weight)
	}

	if shipment.Priority != "HIGH" {
		t.Errorf("expected priority HIGH, got %s", shipment.Priority)
	}

	if shipment.Status != "CREATED" {
		t.Errorf("expected status CREATED, got %s", shipment.Status)
	}

	if shipment.ID == "" {
		t.Errorf("expected shipment ID to be generated")
	}
}

func TestCreateShipmentValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "negative weight",
			body: `{
				"origin": "Madrid",
				"destination": "Barcelona",
				"weight": -15,
				"priority": "HIGH"
			}`,
		},
		{
			name: "invalid priority",
			body: `{
				"origin": "Madrid",
				"destination": "Barcelona",
				"weight": 15,
				"priority": "POTATO"
			}`,
		},
		{
			name: "missing origin",
			body: `{
				"destination": "Barcelona",
				"weight": 15,
				"priority": "HIGH"
			}`,
		},
		{
			name: "missing destination",
			body: `{
				"origin": "Madrid",
				"weight": 15,
				"priority": "HIGH"
			}`,
		},
		{
			name: "same origin and destination",
			body: `{
				"origin": "Madrid",
				"destination": "Madrid",
				"weight": 15,
				"priority": "HIGH"
			}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewInMemoryShipmentStore()
            router := setupRouter(store)

			request, _ := http.NewRequest(
				http.MethodPost,
				"/shipments",
				strings.NewReader(test.body),
			)

			request.Header.Set("Content-Type", "application/json")

			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Errorf(
					"expected status %d, got %d",
					http.StatusBadRequest,
					response.Code,
				)
			}
		})
	}
}

func TestGetShipment(t *testing.T) {
	store := NewInMemoryShipmentStore()
    router := setupRouter(store)

	requestBody := `{
		"origin": "Madrid",
		"destination": "Barcelona",
		"weight": 15,
		"priority": "HIGH"
	}`

	// Primero creamos un shipment
	createRequest, _ := http.NewRequest(
		http.MethodPost,
		"/shipments",
		strings.NewReader(requestBody),
	)
	createRequest.Header.Set("Content-Type", "application/json")

	createResponse := httptest.NewRecorder()

	router.ServeHTTP(createResponse, createRequest)

	if createResponse.Code != http.StatusCreated {
		t.Fatalf(
			"failed to create shipment: expected status %d, got %d",
			http.StatusCreated,
			createResponse.Code,
		)
	}

	var createdShipment Shipment

	err := json.Unmarshal(createResponse.Body.Bytes(), &createdShipment)
	if err != nil {
		t.Fatalf("failed to parse created shipment: %v", err)
	}

	// Ahora recuperamos ese mismo shipment
	getRequest, _ := http.NewRequest(
		http.MethodGet,
		"/shipments/"+createdShipment.ID,
		nil,
	)

	getResponse := httptest.NewRecorder()

	router.ServeHTTP(getResponse, getRequest)

	if getResponse.Code != http.StatusOK {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusOK,
			getResponse.Code,
		)
	}

	var retrievedShipment Shipment

	err = json.Unmarshal(getResponse.Body.Bytes(), &retrievedShipment)
	if err != nil {
		t.Fatalf("failed to parse retrieved shipment: %v", err)
	}

	if retrievedShipment.ID != createdShipment.ID {
		t.Errorf(
			"expected shipment ID %s, got %s",
			createdShipment.ID,
			retrievedShipment.ID,
		)
	}
}

func TestGetShipmentNotFound(t *testing.T) {
	store := NewInMemoryShipmentStore()
    router := setupRouter(store)

	request, _ := http.NewRequest(
		http.MethodGet,
		"/shipments/shp_does_not_exist",
		nil,
	)

	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Errorf(
			"expected status %d, got %d",
			http.StatusNotFound,
			response.Code,
		)
	}
}