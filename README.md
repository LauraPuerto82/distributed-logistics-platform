# Distributed Logistics Platform

An event-driven logistics platform built incrementally to explore realistic distributed-system design, failure modes, and reliability patterns.

The platform currently accepts shipment requests through a Go service, persists them in PostgreSQL, and publishes `ShipmentCreated` events to Kafka. Routing and prediction services are the next milestones.

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
                v
        Routing Service
             planned
                |
         RouteCalculated
                v
              Kafka
                |
                v
       Prediction Service
             planned
```

### Current flow

```text
POST /shipments
→ validate request
→ persist shipment in PostgreSQL
→ publish ShipmentCreated to Kafka
→ return 201 Created
```

## 🛠️ Tech Stack

| Area | Technology |
| --- | --- |
| Service | Go |
| HTTP API | Gin |
| Database | PostgreSQL 17 |
| Database driver | pgx |
| Event streaming | Apache Kafka |
| Infrastructure | Docker Compose |
| Testing | Go testing / httptest |

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

## 🧪 Testing

From `services/order-service`:

```bash
go test -v
```

HTTP behavior tests do not require PostgreSQL or Kafka. The service depends on abstractions that allow production infrastructure to be replaced with in-memory/fake implementations during tests:

| Production | Tests |
| --- | --- |
| `PostgresShipmentStore` | `InMemoryShipmentStore` |
| `KafkaEventPublisher` | `FakeEventPublisher` |

## 📍 Current Status

| Capability | Status |
| --- | --- |
| Order Service API | Implemented |
| PostgreSQL persistence | Implemented |
| Kafka infrastructure | Implemented |
| `ShipmentCreated` publication | Implemented |
| Infrastructure-independent behavior tests | Implemented |
| Routing Service | Next |
| `RouteCalculated` event | Planned |
| Prediction Service | Planned |
| Consumer idempotency and reliability patterns | Planned |

## 🗺️ Roadmap

The immediate target is the first complete asynchronous flow:

```text
Order Service
→ ShipmentCreated
→ Kafka
→ Routing Service
→ RouteCalculated
→ Kafka
→ Prediction Service
```

## Architecture Decisions

Architecture decisions, trade-offs, known technical debt, and intentionally deferred improvements are documented separately in [`docs/ARCHITECTURE_DECISIONS.md`](docs/ARCHITECTURE_DECISIONS.md).

This includes the current non-atomic PostgreSQL/Kafka write, which has been deliberately reproduced as a real failure scenario and is tracked there rather than duplicated in this README.
