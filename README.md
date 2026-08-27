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
| Deployment | AWS CLI, ECR, ECS, Secrets Manager, Fargate-compatible task definitions |
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

- Docker
- Docker Compose
- AWS CLI
- PowerShell

> Go, Python 3.12+, and uv are only required when running or testing services directly outside Docker.

### Quick Start

The complete platform can be deployed locally with a single command from the repository root:

```powershell
.\scripts\deploy-local.ps1
```

The deployment script:

- starts PostgreSQL, Kafka, and MiniStack;
- creates or reuses the local ECS cluster and ECR repositories;
- creates or updates application database connection strings in Secrets Manager;
- builds the three application images;
- pushes them to the MiniStack ECR endpoint;
- generates ECS task definitions from templates with the resolved secret ARNs;
- starts Order Service, Routing Service, and Prediction Service as ECS/Fargate-compatible tasks;
- waits for the Order Service health check;
- waits until the Routing and Prediction Kafka consumer groups are stable with active members.

When the platform is ready, the script finishes with:

```text
Local deployment is ready.
```

The Order Service API is available at `http://localhost:8080`.

Current local infrastructure ports:

```text
PostgreSQL  localhost:5434
Kafka       localhost:9092
MiniStack   localhost:4566
```

### End-to-End Validation

After deployment, validate the complete asynchronous workflow with:

```powershell
.\scripts\test-e2e.ps1
```

The script creates a shipment through Order Service and verifies the event-driven flow:

```text
ShipmentCreated
→ RouteCalculated
→ ETAPredicted
```

It polls the Routing and Prediction transactional outboxes until the expected published events are observed or the validation times out.

A successful run ends with:

```text
End-to-end validation passed.
```

### Stop the Environment

```powershell
.\scripts\stop-local.ps1
```

The script stops the ECS application tasks and then stops PostgreSQL, Kafka, and MiniStack. It can also be safely executed when MiniStack is already stopped.

### Running Services Directly

The automated MiniStack deployment above is the recommended way to run the complete platform. For service-level development and debugging, the services can still be run directly against the Docker Compose infrastructure.

Start the shared infrastructure:

```bash
cd infrastructure/docker
docker compose up -d
```

Order Service environment:

```powershell
$env:DATABASE_URL="postgres://logistics:logistics@localhost:5434/logistics"
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

Run Order Service:

```bash
cd services/order-service
go run .
```

Routing Service environment:

```powershell
$env:DATABASE_URL="postgres://logistics:logistics@localhost:5434/logistics"
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

Run Routing Service in a separate terminal:

```bash
cd services/routing-service
go run .
```

Prediction Service environment:

```powershell
$env:DATABASE_URL="postgresql://logistics:logistics@localhost:5434/logistics"
$env:KAFKA_BROKER="localhost:9092"
$env:KAFKA_TOPIC="shipment-events"
```

Run Prediction Service in another terminal:

```bash
cd services/prediction-service
uv sync
uv run prediction-service
```

> The PostgreSQL schema is currently created manually. Database migrations are planned.

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
| ECS/Fargate task-definition templates | Implemented for all services |
| Secrets Manager integration | Implemented (MiniStack) |
| Automated local MiniStack deployment | Implemented |
| Kafka consumer-group readiness checks | Implemented |
| Automated end-to-end validation | Implemented |
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

The platform is fully containerized and runs end to end locally. AWS deployment concepts are exercised through MiniStack rather than a real AWS deployment.

Local deployment is automated through PowerShell scripts. PostgreSQL, Kafka, and MiniStack run through Docker Compose, while the three application images are built and pushed to MiniStack's local ECR endpoint. Database connection strings are stored in MiniStack Secrets Manager and injected into ECS task definitions generated from version-controlled templates at deployment time.

Order Service, Routing Service, and Prediction Service run as ECS/Fargate-compatible tasks. Deployment readiness is not based only on task startup: the deployment waits for the Order Service health endpoint and for the Routing and Prediction Kafka consumer groups to reach a stable state with active members before reporting the platform as ready.

The complete asynchronous workflow is reproducibly validated through `scripts/test-e2e.ps1`. The script creates a shipment through Order Service and polls the transactional outboxes until published `RouteCalculated` and `ETAPredicted` events are observed.

ECS Service-based execution was also explored for the long-running Order Service workload. MiniStack successfully created and initially ran the service task, but did not reproduce ECS task replacement after the task was manually stopped. This is treated as a local-emulation limitation rather than validated ECS reconciliation behavior.

## Architecture Decisions

Architecture decisions, trade-offs, known technical debt, and intentionally deferred improvements are documented separately in [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md).

This includes reliability trade-offs around PostgreSQL/Kafka coordination, persistent consumer idempotency, transactional outbox delivery in both Routing and Prediction, intentionally accepted at-least-once delivery semantics, explicit Kafka offset-commit behavior in both consumers, permanent/transient failure classification, bounded retries with exponential backoff, and dead-letter handling.
