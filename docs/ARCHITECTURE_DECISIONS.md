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

The same pattern is now used by Routing Service for `RouteCalculated`
publication. Routing depends on the publication capability through its
own `EventPublisher`, allowing route processing to be tested with a
`FakeEventPublisher` without a running Kafka broker.

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

## ADR-009 --- Use a dedicated consumer group for Routing Service

**Status:** Accepted\
**Stage:** Routing Service Kafka integration

### Context

Routing Service is the first real Kafka consumer in the platform. It
consumes `ShipmentCreated` events from `shipment-events` and must be
able to resume consumption across service restarts.

Kafka consumer groups also provide the mechanism for distributing
partitions between multiple instances of the same logical consumer.

### Decision

Routing Service consumes events using:

``` text
GroupID = routing-service
```

Kafka therefore tracks committed offsets for this consumer group and can
resume consumption from its recorded progress.

If multiple Routing Service instances use the same group, Kafka can
distribute topic partitions between those instances rather than
delivering every event to every instance.

### Trade-off

Consumer-group offset tracking provides consumption progress and work
distribution, but it does not make event processing idempotent.

An event may still be delivered more than once around failures or offset
commits, so consumers must eventually tolerate duplicate delivery.

### Alternatives considered

-   Consume without a consumer group and manage offsets directly.
-   Give every Routing Service instance a different group ID, causing
    each instance to receive its own copy of the event stream.

### Consequences

The logical Routing Service consumer has a stable identity and
Kafka-managed progress.

Consumer idempotency remains a separate reliability concern.

------------------------------------------------------------------------

## ADR-010 --- Use a shared `shipment-events` topic for the MVP

**Status:** Accepted for MVP\
**Stage:** Routing Service Kafka integration

### Context

The shipment lifecycle now contains multiple event types:

``` text
ShipmentCreated
RouteCalculated
```

Prediction Service now adds a third shipment lifecycle event:

```text
ETAPredicted
```

All three event types currently share the same topic.

The platform could use a separate Kafka topic for each event type or
keep related shipment lifecycle events in a shared topic.

### Decision

For the current MVP, publish shipment lifecycle events to:

``` text
shipment-events
```

Routing Service consumes this topic and processes only events whose
`event_type` is `ShipmentCreated`.

Prediction Service consumes the same topic through its own consumer group
and processes only events whose `event_type` is `RouteCalculated`.

Other event types, including events published by each service itself, are
ignored by consumers that do not handle them.

Shipment-related messages continue to use `shipment_id` as the Kafka
message key.

### Trade-off

A shared topic keeps the initial Kafka topology simple and groups
related shipment lifecycle events together.

In return, consumers receive event types they may not handle and must
inspect `event_type` before processing a message.

As the number of event types, consumers, or operational requirements
grows, separate topics may provide clearer ownership and independent
retention, scaling, or access policies.

### Alternatives considered

-   Create a dedicated topic for each event type.
-   Create separate topics for each producing service.

### Consequences

The current event flow can evolve without adding a new Kafka topic for
every step.

The topic strategy should be revisited if concrete scaling, ownership,
retention, or operational requirements make the shared topic a
limitation.

------------------------------------------------------------------------


## ADR-011 --- Implement Prediction Service in Python with `uv`

**Status:** Accepted\
**Stage:** Prediction Service baseline

### Context

The first two services are implemented in Go. Prediction Service introduces
a workload expected to evolve toward applied AI/ML capabilities.

### Decision

Implement Prediction Service in Python 3.12 and manage its project
environment and locked dependencies with `uv`.

### Trade-off

The platform becomes polyglot, adding language-specific tooling and
dependency-management concerns.

That cost is accepted because Python provides a natural path for prediction
and future AI/ML capabilities while service boundaries remain explicit.

### Consequences

Each service owns its runtime and dependencies. Kafka event contracts remain
the integration boundary between Go and Python services.

------------------------------------------------------------------------

## ADR-012 --- Start ETA prediction with an explicit heuristic baseline

**Status:** Accepted for MVP\
**Stage:** Prediction Service baseline

### Context

Prediction Service needs to produce a useful ETA before historical or
real-time data exists to justify a trained predictive model.

Introducing ML without suitable data would add complexity without evidence
that the model is meaningful.

### Decision

Use a deterministic MVP baseline:

```text
average speed = 80 km/h
operational buffer = 15%
estimated minutes = (distance / average speed) × buffer × 60
```

The baseline is intentionally described as a heuristic rather than as
machine learning.

### Trade-off

The estimate does not yet account for road type, traffic, construction,
vehicle condition, mandatory driver rest, meal stops, weather, or other
real-world factors.

### Planned evolution

Replace or enrich the baseline when suitable data and requirements justify
a more realistic predictive approach.

------------------------------------------------------------------------

## ADR-013 --- Validate Prediction Service event contracts with Pydantic

**Status:** Accepted\
**Stage:** Prediction Service baseline

### Context

Prediction Service receives JSON produced by another independently running
service. Invalid event types or impossible values should be rejected at the
service boundary rather than silently entering prediction logic.

### Decision

Represent `RouteCalculated` and `ETAPredicted` contracts as Pydantic models.

Current examples include:

```text
RouteCalculated.event_type == "RouteCalculated"
distance_km >= 0
ETAPredicted.event_type == "ETAPredicted"
estimated_travel_minutes >= 0
```

### Trade-off

The Python service maintains its own representation of event contracts, so
cross-language schema drift remains possible until explicit schema
governance is introduced.

### Consequences

Parsing and validation are explicit, typed, and independently testable.

------------------------------------------------------------------------

## ADR-014 --- Give Prediction Service an independent consumer group and replay history on first start

**Status:** Accepted for MVP\
**Stage:** Prediction Service Kafka integration

### Context

Prediction Service must consume `RouteCalculated` independently from
Routing Service. When introduced into an existing development topic, route
events may already exist before the new consumer group has committed an
offset.

### Decision

Use:

```text
group.id = prediction-service
auto.offset.reset = earliest
```

`earliest` applies only when the consumer group has no committed offset.
After progress has been committed, normal restarts resume from the group's
recorded position rather than replaying the entire topic.

### Trade-off

A newly created consumer group may process retained historical events. This
is appropriate for the current service but would need to be reconsidered
for consumers whose side effects should apply only to new events.

### Consequences

Routing Service and Prediction Service consume the shared stream
independently and Kafka tracks their progress separately.

------------------------------------------------------------------------

## ADR-015 --- Keep Prediction Service event publication behind an `EventPublisher` protocol

**Status:** Accepted\
**Stage:** Prediction Service Kafka publication

### Context

Calling `confluent-kafka` directly from prediction logic would couple core
behavior to Kafka and make behavior tests depend on a running broker.

### Decision

Define the publication capability through a Python `Protocol`.
Production uses `KafkaEventPublisher`; tests use a `FakeEventPublisher`.

### Trade-off

This introduces an additional abstraction for a small service.

The abstraction is accepted because Kafka is an external infrastructure
boundary and the fake allows both successful publication and publication
failure behavior to be tested deterministically.

### Consequences

ETA calculation and orchestration can be tested without Kafka, while the
production implementation remains replaceable.

------------------------------------------------------------------------

## ADR-016 --- Flush each `ETAPredicted` publication synchronously for the MVP

**Status:** Accepted for MVP\
**Stage:** Prediction Service Kafka publication

### Context

`confluent-kafka` producers enqueue messages asynchronously. Calling
`produce()` alone does not mean all pending producer messages have completed
delivery before the application continues.

### Decision

Call `flush()` after each `ETAPredicted` publication.

For the current MVP, predictable and easy-to-reason-about delivery behavior
is preferred over maximum producer throughput.

### Trade-off

Flushing after every event introduces synchronous waiting and prevents the
producer from taking full advantage of batching and asynchronous throughput.

This decision does not provide exactly-once processing and does not solve
the coordination between downstream publication and the consumed event's
offset commit.

### Planned evolution

If throughput becomes relevant, use asynchronous delivery callbacks and
batching or controlled flushing while defining explicit failure and
shutdown semantics.

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
Routing Service and Prediction Service are now real Kafka consumers, but
neither currently persists which events it has already processed.

If an input event is delivered more than once, the corresponding service
may process it again and publish a duplicate downstream event
(`RouteCalculated` or `ETAPredicted`).

**Planned evolution:**\
Use `event_id` to detect events that have already been processed and
make consumer side effects safe under duplicate delivery.

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

## TD-009 --- Consumer offset commit semantics are not explicitly controlled

**Area:** Event processing reliability\
**Priority:** High

**Current limitation:**\
Routing Service and Prediction Service currently rely on their Kafka
clients' consumer-group behavior for offset management. The application
has not yet defined an explicit policy for when a consumed event should
be considered successfully processed relative to publishing its
downstream event.

A failure around event processing, publication, and offset commit can
lead to redelivery or other ambiguous processing outcomes.

**Planned evolution:**\
Define explicit consumer processing and offset-commit semantics together
with retry and idempotency behavior, so failures during
`RouteCalculated` or `ETAPredicted` publication can be handled safely.

------------------------------------------------------------------------


## TD-010 --- Prediction producer flushes synchronously per event

**Area:** Messaging throughput\
**Priority:** Low

**Current limitation:**\
Prediction Service calls `flush()` after every `ETAPredicted` publication.
This keeps MVP delivery behavior simple but adds synchronous waiting and
limits producer throughput.

**Planned evolution:**\
If throughput requirements justify it, move to asynchronous delivery
callbacks and batching or controlled flushing while preserving explicit
error handling.

**Related decision:** ADR-016

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

## Retry and dead-letter strategy

Retries, backoff, and dead-letter handling will be designed after the
first consumer provides a concrete failure model.

## Observability and distributed tracing

Logging, correlation IDs, metrics, and tracing will evolve when
requests/events begin crossing multiple running services and local logs
are no longer sufficient.

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
