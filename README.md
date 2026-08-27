# Distributed Logistics Platform

An event-driven logistics platform built incrementally to explore realistic distributed-system design, failure modes, and reliability patterns.

The platform currently accepts shipment requests through a Go service, persists them in PostgreSQL, and publishes `ShipmentCreated` events to Kafka. The Routing Service consumes those events idempotently, calculates the shortest route, and reliably publishes `RouteCalculated` through a transactional outbox. The Python Prediction Service consumes `RouteCalculated` events idempotently, estimates travel time using an explicit MVP baseline, and reliably publishes `ETAPredicted` through its own transactional outbox.

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
              | RouteCalculated
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
                       | processed event +
                       | ETAPredicted
                       v
                  PostgreSQL
                 /          \
        processed_events    outbox_events
                                 |
                                 | pending ETAPredicted
                                 v
                          Outbox Publisher
                                 |
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
→ commit the consumed Kafka offset after successful processing
→ Routing Outbox Publisher reads pending events
→ publish RouteCalculated to Kafka
→ mark outbox event as published
→ Prediction Service consumes RouteCalculated
→ validate the event contract with Pydantic
→ check whether the event was already processed
→ estimate travel time
→ atomically persist processed event + ETAPredicted outbox event
→ commit the consumed Kafka offset after successful processing
→ Prediction Outbox Publisher reads pending events
→ publish ETAPredicted to Kafka
→ mark outbox event as published
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
| Containerization | Docker |
| Local orchestration | Docker Compose |
| Local AWS emulation | MiniStack |
| Deployment | AWS CLI, ECR, ECS, Fargate-compatible task definitions |
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

The Prediction Service uses PostgreSQL to persist processed event IDs and transactional outbox events, and Kafka to consume and publish domain events.

PowerShell:

```powershell
$env:DATABASE_URL="postgres://logistics:logistics@localhost:5434/logistics"
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

The service validates incoming `RouteCalculated` events with Pydantic, processes them idempotently, estimates travel time using the current MVP baseline, and atomically persists the processed input event together with the resulting `ETAPredicted` outbox event. A background outbox publisher then publishes pending events to Kafka and marks them as published after successful delivery.

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

# Unit tests
uv run pytest -m "not integration" -v

# Integration tests (requires PostgreSQL and DATABASE_URL)
uv run pytest -m integration -v
```

Tests are designed to run without PostgreSQL or Kafka where possible. Infrastructure dependencies are abstracted behind interfaces and replaced with in-memory or fake implementations during unit tests.

| Production | Tests |
| --- | --- |
| `PostgresShipmentStore` | `InMemoryShipmentStore` |
| `KafkaEventPublisher` | `FakeEventPublisher` |
| `PostgresRoutingStore` (`ProcessedEventStore`) | `FakeProcessedEventStore` |
| `PostgresRoutingStore` (`OutboxStore`) | `FakeOutboxStore` |
| `PostgresPredictionStore` (`ProcessedEventStore`) | `FakeProcessedEventStore` |
| `PostgresPredictionStore` (`OutboxStore`) | `FakeOutboxStore` |

Routing Service separates event-processing, outbox persistence, and event-publication behavior behind `ProcessedEventStore`, `OutboxStore`, and `EventPublisher`. Unit tests use fakes to verify idempotent processing, outbox event creation, publication, and at-least-once retry behavior without requiring PostgreSQL or Kafka.

PostgreSQL-specific outbox behavior is covered separately by integration tests using the `integration` build tag.

Prediction Service follows the same reliability-oriented approach through `ProcessedEventStore`, `OutboxStore`, and `EventPublisher` protocols. Unit tests use fakes to verify idempotent `RouteCalculated` processing, transactional outbox behavior, publication failure handling, and at-least-once retry semantics without requiring PostgreSQL or Kafka.

PostgreSQL-specific behavior is covered separately by integration tests, including atomic persistence of processed events and outbox events, rollback on partial failure, pending-event retrieval, and publication-state updates.

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
| Routing explicit Kafka offset commits | Implemented |
| Routing permanent/transient failure classification | Implemented |
| Routing bounded retries with exponential backoff | Implemented |
| Routing dead-letter handling | Implemented |
| Routing transactional outbox | Implemented |
| `RouteCalculated` publication | Implemented |
| Routing PostgreSQL integration tests | Implemented |
| Infrastructure-independent behavior tests | Implemented |
| Prediction Service | Implemented |
| `RouteCalculated` consumption | Implemented |
| ETA baseline prediction | Implemented |
| Prediction consumer idempotency | Implemented |
| Prediction explicit Kafka offset commits | Implemented |
| Prediction invalid-message classification | Implemented |
| Prediction bounded retries with exponential backoff | Implemented |
| Prediction dead-letter handling | Implemented |
| Prediction transactional outbox | Implemented |
| `ETAPredicted` publication | Implemented |
| Prediction PostgreSQL integration tests | Implemented |
| Docker images for all three services | Implemented |
| Full-platform Docker Compose execution | Implemented |
| Persistent local MiniStack environment | Implemented |
| Local ECR push/pull workflow | Implemented |
| Order Service ECS task definition | Implemented (MiniStack) |
| Routing Service ECS task definition | Implemented (MiniStack) |
| Prediction Service ECS task definition | Implemented (MiniStack) |
| All application services ECS/Fargate task execution | Implemented and E2E validated (MiniStack) |
| ECS Service-based deployment | Partially validated (MiniStack limitation identified) |

## 🗺️ Roadmap

The asynchronous MVP flow is now implemented end to end:

```text
Order Service
→ ShipmentCreated
→ Kafka
→ Routing Service
→ idempotent processing
→ transactional outbox
→ RouteCalculated
→ Kafka
→ Prediction Service
→ idempotent processing
→ transactional outbox
→ ETAPredicted
→ Kafka
```

The Order → Routing → Prediction flow is implemented. Both Routing and Prediction now provide persistent consumer idempotency and transactional outbox publication with at-least-once delivery semantics.

Both Routing and Prediction use explicit Kafka offset commits. A consumed event is committed only after successful processing and durable PostgreSQL persistence. If the offset commit fails after processing succeeds, persistent `event_id`-based idempotency makes redelivery safe.

Both consumers now apply explicit failure-handling policies. Malformed JSON is treated as a terminal input failure in both services. Routing also treats known deterministic route-processing failures as permanent, while Prediction treats invalid `RouteCalculated` events rejected by Pydantic as terminal. These failures are sent directly to their service-specific dead-letter topics without retrying.

Other processing failures are treated as transient. Both services perform a maximum of three processing attempts with exponential backoff between attempts. If all attempts are exhausted, the original message is sent to the corresponding DLQ.

The original Kafka offset is committed only after processing reaches a terminal outcome: either successful processing or successful dead-letter publication. If DLQ publication fails, the original offset remains uncommitted so Kafka can redeliver the message. If DLQ publication succeeds but the later offset commit fails, redelivery may produce a duplicate dead-letter record, which is intentionally accepted as at-least-once DLQ delivery.

### Deployment

The platform is fully containerized and runs end to end with Docker Compose. AWS deployment is being introduced incrementally using MiniStack as a local emulator.

The local ECS deployment has now been extended to all three application services. Order Service, Routing Service, and Prediction Service each have a Fargate-compatible task definition and their images are stored in local ECR. PostgreSQL and Kafka remain managed by Docker Compose as shared infrastructure.

The complete event flow has been validated end to end with all three application services running as ECS-managed tasks: a shipment created through Order Service was processed by Routing Service and Prediction Service through Kafka, producing published `RouteCalculated` and `ETAPredicted` outbox events.

ECS Service-based execution was also explored for the long-running Order Service workload. MiniStack successfully created and initially ran the service task, but did not reproduce ECS task replacement after the task was manually stopped. This is treated as a local-emulation limitation rather than validated ECS reconciliation behavior.

## Architecture Decisions

Architecture decisions, trade-offs, known technical debt, and intentionally deferred improvements are documented separately in [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md).

This includes reliability trade-offs around PostgreSQL/Kafka coordination, persistent consumer idempotency, transactional outbox delivery in both Routing and Prediction, intentionally accepted at-least-once delivery semantics, explicit Kafka offset-commit behavior in both consumers, permanent/transient failure classification, bounded retries with exponential backoff, and dead-letter handling.
