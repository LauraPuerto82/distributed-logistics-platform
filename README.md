# Distributed Logistics Platform

An event-driven logistics platform built incrementally to explore realistic distributed-system design, failure modes, and reliability patterns.

The platform currently accepts shipment requests through a Go service, persists them in PostgreSQL, and publishes `ShipmentCreated` events to Kafka. The Routing Service consumes those events, calculates the shortest route, and publishes `RouteCalculated`. The Python Prediction Service consumes the calculated route, estimates travel time using an explicit MVP baseline, and publishes `ETAPredicted` back to Kafka.

## 🏗️ Architecture

```text
    Client
      |
      | POST /shipments
      v
Order Service (Go)
      |    \
      |     \ ShipmentCreated
      |      \
      v       v
PostgreSQL   Kafka
               |
               | ShipmentCreated
               v
        Routing Service
               |
               | RouteCalculated
               v
             Kafka
               |
               v
       Prediction Service
               |
               | ETAPredicted
               v
             Kafka
```

### Current flow

```text
POST /shipments
→ validate request
→ persist shipment in PostgreSQL
→ publish ShipmentCreated to Kafka
→ Routing Service consumes ShipmentCreated
→ calculate shortest route
→ publish RouteCalculated to Kafka
→ Prediction Service consumes RouteCalculated
→ validate the event contract with Pydantic
→ estimate travel time
→ publish ETAPredicted to Kafka
```

## 🛠️ Tech Stack

| Area | Technology |
| --- | --- |
| Services | Go, Python 3.12 |
| Python project/dependency management | uv |
| Python validation | Pydantic |
| HTTP API | Gin |
| Database | PostgreSQL 17 |
| Database driver | pgx |
| Event streaming | Apache Kafka |
| Infrastructure | Docker Compose |
| Testing | Go testing / httptest / pytest |

## 🔌 API

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/health` | Service health check |
| `POST` | `/shipments` | Create a shipment |
| `GET` | `/shipments/:id` | Retrieve a shipment by ID |

Example request:

```json
{
  "origin": "Zaragoza",
  "destination": "Bilbao",
  "weight": 20,
  "priority": "MEDIUM"
}
```

A successful creation persists the shipment and publishes a `ShipmentCreated` event to Kafka.

## 🚀 Running Locally

### Prerequisites

- Go
- Python 3.12+
- uv
- Docker
- Docker Compose

### 1. Start infrastructure

```bash
cd infrastructure/docker
docker compose up -d
```

Current local ports:

```text
PostgreSQL  localhost:5434
Kafka       localhost:9092
```

### 2. Configure Order Service

PowerShell:

```powershell
$env:DATABASE_URL="postgres://logistics:logistics@localhost:5434/logistics"
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

### 3. Run Order Service

```bash
cd services/order-service
go run .
```

The API runs on `http://localhost:8080`.

> The PostgreSQL schema is currently created manually. Database migrations are planned.

### 4. Configure Routing Service

The Routing Service consumes and publishes events through the same Kafka topic.

PowerShell:

```powershell
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

### 5. Run Routing Service

Open a separate terminal:

```bash
cd services/routing-service
go run .
```

The service consumes `ShipmentCreated` events, calculates the shortest route using Dijkstra's algorithm, and publishes a `RouteCalculated` event back to Kafka.

### 6. Configure Prediction Service

The Prediction Service consumes `RouteCalculated` events and publishes `ETAPredicted` events through the same Kafka topic.

PowerShell:

```powershell
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

### 7. Run Prediction Service

Open another terminal:

```bash
cd services/prediction-service
uv sync
uv run prediction-service
```

The service validates incoming `RouteCalculated` events with Pydantic, estimates travel time using the current MVP baseline, and publishes an `ETAPredicted` event back to Kafka.

## 🧪 Testing

Run each service's tests independently.

Order Service:

```bash
cd services/order-service
go test -v
```

Routing Service:

```bash
cd services/routing-service
go test -v
```

Prediction Service:

```bash
cd services/prediction-service
uv run pytest -v
```

Tests are designed to run without PostgreSQL or Kafka where possible. Infrastructure dependencies are abstracted behind interfaces and replaced with in-memory or fake implementations during tests.

| Production | Tests |
| --- | --- |
| `PostgresShipmentStore` | `InMemoryShipmentStore` |
| `KafkaEventPublisher` | `FakeEventPublisher` |

Routing Service follows the same approach through `EventPublisher`, allowing route calculation and event publication behavior to be tested with a `FakeEventPublisher` without a running Kafka broker.

Prediction Service follows the same approach through a Python `EventPublisher` protocol, allowing ETA calculation and event publication behavior to be tested with a `FakeEventPublisher` without a running Kafka broker.

## 📍 Current Status

| Capability | Status |
| --- | --- |
| Order Service API | Implemented |
| PostgreSQL persistence | Implemented |
| Kafka infrastructure | Implemented |
| `ShipmentCreated` publication | Implemented |
| Routing Service | Implemented |
| Shortest-path routing (Dijkstra) | Implemented |
| `ShipmentCreated` consumption | Implemented |
| `RouteCalculated` publication | Implemented |
| Infrastructure-independent behavior tests | Implemented |
| Prediction Service | Implemented |
| `RouteCalculated` consumption | Implemented |
| ETA baseline prediction | Implemented |
| `ETAPredicted` publication | Implemented |
| Consumer idempotency and reliability patterns | Planned |

## 🗺️ Roadmap

The asynchronous MVP flow is now implemented end to end:

```text
Order Service
→ ShipmentCreated
→ Kafka
→ Routing Service
→ RouteCalculated
→ Kafka
→ Prediction Service
→ ETAPredicted
→ Kafka
```

The Order → Routing → Prediction flow is implemented. The next reliability milestone is to define explicit consumer offset-commit semantics, retries, and idempotent processing.

## Architecture Decisions

Architecture decisions, trade-offs, known technical debt, and intentionally deferred improvements are documented separately in [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md).

This includes the current non-atomic PostgreSQL/Kafka write, which has been deliberately reproduced as a real failure scenario and is tracked there rather than duplicated in this README.
