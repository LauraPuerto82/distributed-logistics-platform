# Distributed Logistics Platform

An event-driven logistics platform built incrementally to explore realistic distributed-system design, failure modes, and reliability patterns.

The platform currently accepts shipment requests through a Go service, persists them in PostgreSQL, and publishes `ShipmentCreated` events to Kafka. The Routing Service consumes those events idempotently, calculates the shortest route, and reliably publishes `RouteCalculated` through a transactional outbox. The Python Prediction Service consumes the calculated route, estimates travel time using an explicit MVP baseline, and publishes `ETAPredicted` back to Kafka.

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
              | processed event +
              | outbox event
              v
         PostgreSQL
        /          \
processed_events  outbox_events
                       |
                       | pending RouteCalculated
                       v
                Outbox Publisher
                       |
                       v
                     Kafka
                       |
                       | RouteCalculated
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
→ check whether the event was already processed
→ calculate shortest route
→ atomically persist processed event + RouteCalculated outbox event
→ Outbox Publisher reads pending events
→ publish RouteCalculated to Kafka
→ mark outbox event as published
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

The Routing Service uses PostgreSQL to persist processed event IDs and transactional outbox events, and Kafka to consume and publish domain events.

PowerShell:

```powershell
$env:DATABASE_URL="postgres://logistics:logistics@localhost:5434/logistics"
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

### 5. Run Routing Service

Open a separate terminal:

```bash
cd services/routing-service
go run .
```

The service consumes `ShipmentCreated` events idempotently, calculates the shortest route using Dijkstra's algorithm, and stores the resulting `RouteCalculated` event in a transactional outbox. A background outbox publisher then publishes pending events to Kafka and marks them as published after successful delivery.

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

# Unit tests
go test -v

# Integration tests (requires PostgreSQL)
go test -tags=integration -v
```

Prediction Service:

```bash
cd services/prediction-service
uv run pytest -v
```

Tests are designed to run without PostgreSQL or Kafka where possible. Infrastructure dependencies are abstracted behind interfaces and replaced with in-memory or fake implementations during unit tests.

| Production | Tests |
| --- | --- |
| `PostgresShipmentStore` | `InMemoryShipmentStore` |
| `KafkaEventPublisher` | `FakeEventPublisher` |
| `PostgresRoutingStore` (`ProcessedEventStore`) | `FakeProcessedEventStore` |
| `PostgresRoutingStore` (`OutboxStore`) | `FakeOutboxStore` |

Routing Service separates event-processing, outbox persistence, and event-publication behavior behind `ProcessedEventStore`, `OutboxStore`, and `EventPublisher`. Unit tests use fakes to verify idempotent processing, outbox event creation, publication, and at-least-once retry behavior without requiring PostgreSQL or Kafka.

PostgreSQL-specific outbox behavior is covered separately by integration tests using the `integration` build tag.

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
| Routing consumer idempotency | Implemented |
| Routing transactional outbox | Implemented |
| `RouteCalculated` publication | Implemented |
| Routing PostgreSQL integration tests | Implemented |
| Infrastructure-independent behavior tests | Implemented |
| Prediction Service | Implemented |
| `RouteCalculated` consumption | Implemented |
| ETA baseline prediction | Implemented |
| `ETAPredicted` publication | Implemented |
| Prediction consumer idempotency and reliability | Planned |

## 🗺️ Roadmap

The asynchronous MVP flow is now implemented end to end:

```text
Order Service
→ ShipmentCreated
→ Kafka
→ Routing Service
→ transactional outbox
→ RouteCalculated
→ Kafka
→ Prediction Service
→ ETAPredicted
→ Kafka
```

The Order → Routing → Prediction flow is implemented. Routing now provides persistent consumer idempotency and transactional outbox publication with at-least-once delivery semantics.

The next reliability milestone is to extend idempotent processing and reliable event publication to the Prediction Service, then define explicit Kafka offset-commit and retry semantics across consumers.

## Architecture Decisions

Architecture decisions, trade-offs, known technical debt, and intentionally deferred improvements are documented separately in [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md).

This includes reliability trade-offs around PostgreSQL/Kafka coordination, transactional outbox delivery, consumer idempotency, and intentionally accepted at-least-once delivery semantics.
