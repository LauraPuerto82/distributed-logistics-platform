# Architecture Decisions & Known Technical Debt

This document records significant architectural decisions, trade-offs,
and known technical limitations in the **Distributed Logistics
Platform**.

The goal is not to document every implementation detail. It is to make
important engineering decisions explicit: **what problem existed, what
was decided, why, which alternatives were considered, and what
limitations are intentionally being accepted at the current stage of the
project.**

The system is being built incrementally. Some decisions are
intentionally appropriate for the current MVP and may evolve as new
requirements or failure modes appear.

------------------------------------------------------------------------

# Architecture Decisions

## ADR-001 --- Build the system incrementally instead of implementing the target architecture upfront

**Status:** Accepted\
**Stage:** Initial development

### Context

The target platform is intended to evolve into a distributed,
event-driven system with multiple services and infrastructure
components.

It would have been possible to introduce the full target architecture
from the beginning, including persistence abstractions, PostgreSQL,
Kafka, multiple services, reliability mechanisms, and additional
infrastructure.

Doing so would add significant complexity before the problems requiring
those components had actually appeared.

### Decision

Build the platform incrementally.

Start with the smallest implementation that satisfies the current
requirement and introduce additional infrastructure or abstractions when
a concrete need justifies them.

Examples so far include:

-   starting shipment storage in memory;
-   introducing PostgreSQL when persistence became necessary;
-   introducing `ShipmentStore` when infrastructure coupling affected
    testability;
-   introducing `EventPublisher` when Order Service gained
    responsibility for publishing events;
-   postponing advanced reliability patterns until the failure modes
    they address have been observed.

### Trade-off

The architecture is not the final target architecture at every
intermediate commit.

Some implementations are deliberately temporary and will be replaced or
evolved later.

In return, each architectural component has a concrete reason to exist
and its cost can be evaluated against an actual problem.

### Alternatives considered

-   Design and implement the complete target architecture before
    developing the first vertical flow.
-   Introduce common enterprise patterns preemptively.

### Consequences

Positive:

-   lower initial complexity;
-   architectural decisions are tied to observed requirements;
-   easier learning and debugging;
-   smaller changes with clearer intent;
-   less speculative abstraction.

Negative:

-   some code is intentionally transitional;
-   refactoring is expected as the system evolves.

------------------------------------------------------------------------

## ADR-002 --- Abstract shipment persistence behind `ShipmentStore`

**Status:** Accepted\
**Stage:** PostgreSQL integration

### Context

The first version of Order Service stored shipments in memory.

When PostgreSQL persistence was introduced, directly using PostgreSQL
from HTTP handlers would couple request handling to a specific storage
technology and make HTTP tests depend on a running PostgreSQL instance.

### Decision

Define shipment persistence through the `ShipmentStore` interface.

Current implementations:

``` text
ShipmentStore
├── InMemoryShipmentStore
└── PostgresShipmentStore
```

Production uses PostgreSQL while tests can use the in-memory
implementation.

### Trade-off

The abstraction adds an additional layer and interface to a service that
could technically call PostgreSQL directly.

That additional complexity is accepted because it provides concrete
benefits: infrastructure independence and testability.

### Alternatives considered

-   Call `pgx` directly from Gin handlers.
-   Run PostgreSQL for every HTTP test.
-   Mock the PostgreSQL client directly.

### Consequences

HTTP handlers depend on the persistence capability they need rather than
on PostgreSQL itself.

The storage implementation can change without changing the HTTP contract
or core handler behavior.

------------------------------------------------------------------------

## ADR-003 --- Keep external infrastructure out of HTTP behavior tests

**Status:** Accepted\
**Stage:** PostgreSQL and Kafka integration

### Context

Order Service currently depends on external infrastructure in
production: PostgreSQL and Kafka.

Requiring these systems for every HTTP behavior test would make the test
suite slower, more fragile, and dependent on local infrastructure state.

### Decision

Inject infrastructure dependencies into the router.

Use lightweight test implementations:

``` text
Production                    Tests

PostgresShipmentStore         InMemoryShipmentStore
KafkaEventPublisher           NoOp/Fake EventPublisher
```

Tests that specifically verify PostgreSQL or Kafka integration should be
treated separately from tests of HTTP behavior.

### Trade-off

Passing behavior tests do not prove that PostgreSQL or Kafka integration
works. Dedicated integration testing is still required for
infrastructure boundaries.

### Consequences

Tests remain fast, deterministic, and runnable while external
infrastructure is unavailable.

------------------------------------------------------------------------

## ADR-004 --- Protect selected domain invariants at both API and database boundaries

**Status:** Accepted\
**Stage:** PostgreSQL integration

### Context

Some shipment rules are important for data integrity, including:

-   weight must be greater than zero;
-   priority must belong to the supported set;
-   origin and destination must differ.

Application validation gives clients useful feedback, but PostgreSQL may
eventually receive data through paths other than the current HTTP
handler.

### Decision

Validate relevant input at the application boundary and protect critical
data invariants with PostgreSQL constraints where appropriate.

Examples currently enforced in PostgreSQL include:

``` text
weight > 0
priority IN ('LOW', 'MEDIUM', 'HIGH')
origin <> destination
```

### Trade-off

Some rules exist in more than one layer and therefore require
coordinated maintenance if the domain changes.

This decision does not imply that all business logic should be
duplicated in PostgreSQL.

------------------------------------------------------------------------

## ADR-005 --- Abstract event publication behind `EventPublisher`

**Status:** Accepted\
**Stage:** Kafka integration

### Context

Order Service needs to publish `ShipmentCreated` after creating a
shipment.

Calling `kafka-go` directly from HTTP handlers would couple application
behavior to Kafka and make HTTP tests require a running broker.

### Decision

Define event publication through:

``` go
type EventPublisher interface {
    PublishShipmentCreated(event ShipmentCreatedEvent) error
}
```

Production uses `KafkaEventPublisher`; tests use a NoOp/Fake publisher.

### Trade-off

The interface adds another abstraction, accepted because event
publication is an external infrastructure boundary and tests require a
controllable implementation.

------------------------------------------------------------------------

## ADR-006 --- Define a dedicated `ShipmentCreated` event contract

**Status:** Accepted\
**Stage:** Kafka integration

### Context

Using the internal `Shipment` struct directly as the Kafka payload would
make the external event contract change whenever the internal
representation changes.

### Decision

Define a dedicated event and payload contract:

``` json
{
  "event_id": "evt_...",
  "event_type": "ShipmentCreated",
  "timestamp": "...",
  "shipment_id": "shp_...",
  "payload": {
    "origin": "...",
    "destination": "...",
    "weight": 0,
    "priority": "..."
  }
}
```

`event_id` identifies the event occurrence. `shipment_id` identifies the
shipment entity.

### Trade-off

Mapping between internal models and event contracts requires additional
code.

### Alternatives considered

-   Serialize the complete internal `Shipment` struct.
-   Publish only the shipment ID.

### Consequences

The event contract is explicit and can evolve independently from
internal storage or HTTP representations. The `event_id` can later
support idempotent consumers.

------------------------------------------------------------------------

## ADR-007 --- Use `shipment_id` as the Kafka message key

**Status:** Accepted\
**Stage:** Kafka integration

### Context

A shipment may eventually generate a sequence of related events:

``` text
ShipmentCreated
RouteCalculated
ETAPredicted
```

Kafka guarantees ordering within a partition.

### Decision

Publish shipment-related events using:

``` text
Kafka message key = shipment_id
```

### Trade-off

Entity affinity may produce uneven partition distribution if workload
characteristics become skewed. That is not currently a concern for the
MVP.

### Consequences

With a key-based partitioner, events for the same shipment can be routed
consistently to the same partition, allowing their relative order to be
preserved as the topic scales.

------------------------------------------------------------------------

## ADR-008 --- Persist the shipment before publishing `ShipmentCreated`

**Status:** Accepted for MVP\
**Stage:** First end-to-end Kafka flow

### Context

Creating a shipment currently requires two independent operations:

``` text
1. Persist shipment in PostgreSQL.
2. Publish ShipmentCreated to Kafka.
```

PostgreSQL and Kafka do not participate in a shared atomic transaction
in the current design.

The following failure was deliberately reproduced:

``` text
PostgreSQL INSERT       SUCCESS
Kafka publish           FAILURE
```

The API returned an error while the shipment remained persisted in
PostgreSQL.

### Decision

For the initial implementation, Order Service will:

1.  validate the request;
2.  persist the shipment in PostgreSQL;
3.  publish `ShipmentCreated`;
4.  return `201 Created` only if both operations report success;
5.  return `500 Internal Server Error` if event publication reports
    failure.

The resulting inconsistency window is consciously accepted for the
current MVP.

### Trade-off

A shipment can be persisted without its corresponding event being
successfully published.

Therefore:

``` text
HTTP response: failure
Database state: shipment exists
Kafka state: event may be absent
```

The system currently has no durable record that publication still needs
to occur.

Additionally, retrying the POST is not currently idempotent because each
request generates a new shipment ID.

### Alternatives considered

#### Return `201 Created` when Kafka publication fails

Not chosen because creation is expected to initiate downstream
asynchronous processing. Returning success would hide the failure to
initiate that processing.

#### Delete the shipment when Kafka publication fails

Not adopted.

A failed publish call does not necessarily prove that Kafka did not
receive the event. A network or acknowledgement failure could make the
result ambiguous.

Deleting the shipment could therefore create the opposite inconsistency:

``` text
Kafka event exists
PostgreSQL shipment does not exist
```

The compensating delete could also fail independently.

#### Check Kafka availability before inserting

Not sufficient. A successful health check only proves Kafka was
available at the moment of the check.

#### Implement Transactional Outbox immediately

Deferred. The current priority is completing and understanding the first
end-to-end event-driven flow before introducing additional reliability
infrastructure.

### Planned evolution

Introduce a **Transactional Outbox**.

Conceptually:

``` text
BEGIN PostgreSQL transaction

INSERT shipment
INSERT outbox event

COMMIT
```

A separate publisher can then retry pending outbox events until Kafka
publication succeeds.

This allows PostgreSQL to atomically persist both the shipment and the
durable intent to publish `ShipmentCreated`.

HTTP request idempotency should be considered separately to make client
retries safe.

### Important note

The current `500` response does **not** mean the database operation was
rolled back.

This behavior is documented intentionally rather than being treated as
an unknown defect.

------------------------------------------------------------------------

# Known Technical Debt

## TD-001 --- Non-atomic PostgreSQL + Kafka writes

**Area:** Messaging consistency\
**Priority:** High

**Current limitation:**\
Persisting a shipment in PostgreSQL and publishing `ShipmentCreated` to
Kafka are two independent operations.

A Kafka failure after the database commit can leave a persisted shipment
without its corresponding event.

**Planned evolution:**\
Introduce a Transactional Outbox.

**Related decision:** ADR-008

------------------------------------------------------------------------

## TD-002 --- `POST /shipments` is not idempotent

**Area:** HTTP API\
**Priority:** Medium

**Current limitation:**\
Each request generates a new shipment ID. If a client retries a request
after an ambiguous failure, a second shipment can be created.

**Planned evolution:**\
Evaluate an `Idempotency-Key` mechanism when retry semantics are
introduced.

**Related decision:** ADR-008

------------------------------------------------------------------------

## TD-003 --- Consumers are not yet idempotent

**Area:** Event processing\
**Priority:** High

**Current limitation:**\
Consumers do not yet track which events they have already processed.

When real consumers are introduced, duplicate delivery could therefore
produce duplicate side effects.

**Planned evolution:**\
Use `event_id` to detect events that have already been processed.

------------------------------------------------------------------------

## TD-004 --- Kafka availability is not verified at startup

**Area:** Service readiness\
**Priority:** Low

**Current limitation:**\
Order Service can start successfully even when Kafka is unavailable. The
problem is currently discovered only when publishing an event.

**Planned evolution:**\
Define readiness semantics and decide whether Kafka connectivity should
be part of the readiness check.

------------------------------------------------------------------------

## TD-005 --- Kafka publish timeout/retry policy is not explicitly defined

**Area:** Messaging reliability\
**Priority:** Medium

**Current limitation:**\
When Kafka is unavailable, publication behavior and the amount of time
an HTTP request may wait are not yet explicitly controlled by our
application.

**Planned evolution:**\
Define bounded timeouts and retry behavior after evaluating the desired
failure semantics.

------------------------------------------------------------------------

## TD-006 --- Database schema changes are manual

**Area:** Database\
**Priority:** Medium

**Current limitation:**\
The initial `shipments` table was created manually in PostgreSQL.

This is sufficient for the current development stage but does not
provide a reproducible schema evolution process.

**Planned evolution:**\
Introduce versioned database migrations when the schema starts evolving.

------------------------------------------------------------------------

## TD-007 --- Infrastructure integration tests are still missing

**Area:** Testing\
**Priority:** Medium

**Current limitation:**\
HTTP behavior tests intentionally use test doubles, so they do not
verify the real PostgreSQL and Kafka integrations.

**Planned evolution:**\
Add dedicated integration tests for infrastructure boundaries.

------------------------------------------------------------------------

## TD-008 --- Event schema versioning is not defined

**Area:** Event contracts\
**Priority:** Low

**Current limitation:**\
Event contracts are currently defined in Go, without an explicit
versioning or schema-governance strategy.

**Planned evolution:**\
Introduce event versioning/schema governance if independently evolving
consumers make it necessary.

------------------------------------------------------------------------

# Deferred Decisions

These topics are intentionally **not decided yet** because the system
does not currently provide enough evidence to justify a specific
implementation.

## Shipment status transitions

The platform will eventually require shipment status changes as
downstream services process events. A generic update API or state
machine has not been introduced yet because no current service requires
it.

## Kafka consumer groups

Consumer group configuration will be decided when the first real Kafka
consumer is implemented.

## Retry and dead-letter strategy

Retries, backoff, and dead-letter handling will be designed after the
first consumer provides a concrete failure model.

## Observability and distributed tracing

Logging, correlation IDs, metrics, and tracing will evolve when
requests/events begin crossing multiple running services and local logs
are no longer sufficient.

## Topic strategy

The MVP currently uses `shipment-events`. The decision to retain a
shared event topic or split event types across topics will be revisited
when multiple producers/consumers create concrete operational
requirements.

------------------------------------------------------------------------

# Decision Principles

1.  **Introduce complexity in response to a concrete problem.**
2.  **Keep external contracts separate from internal implementation
    details.**
3.  **Make infrastructure dependencies explicit and injectable.**
4.  **Keep fast behavior tests independent from external
    infrastructure.**
5.  **Protect important data invariants at appropriate boundaries.**
6.  **Assume distributed operations can fail partially.**
7.  **Design consumers to eventually tolerate duplicate delivery.**
8.  **Document known limitations instead of hiding them.**
9.  **Distinguish an acceptable MVP trade-off from a production-ready
    reliability guarantee.**
10. **Revisit decisions when new evidence or requirements invalidate
    their assumptions.**

------------------------------------------------------------------------

# Status Legend

-   **Proposed** --- under consideration.
-   **Accepted** --- current architectural decision.
-   **Accepted for MVP** --- consciously temporary decision appropriate
    for the current stage.
-   **Superseded** --- replaced by a later ADR.
-   **Deprecated** --- should no longer be used for new development.

When a decision changes, keep the original entry and mark it as
superseded rather than rewriting history.
