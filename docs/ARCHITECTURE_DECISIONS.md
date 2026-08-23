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

The same abstraction is also used by Routing Service for Kafka publication.
Routing's outbox publisher depends on its own `EventPublisher`, allowing
outbox publication behavior to be tested with a `FakeEventPublisher`
without a running Kafka broker.

Route processing itself no longer publishes directly through this interface.
It persists the resulting event in a transactional outbox, which is published
separately.

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

## ADR-017 --- Persist Routing Service consumer idempotency in PostgreSQL

**Status:** Accepted\
**Stage:** Routing Service reliability

### Context

Kafka consumers may receive the same event more than once around failures,
restarts, retries, or offset commits.

Routing Service therefore cannot assume that each `ShipmentCreated` event is
delivered exactly once. Reprocessing the same event could repeat route
calculation and create duplicate downstream effects.

An in-memory record of processed event IDs would prevent duplicates only while
a single process remains alive and would be lost on restart.

### Decision

Persist processed input `event_id` values in PostgreSQL.

Routing Service checks whether an event has already been processed before
performing its business logic. Successfully processed input events are recorded
in `routing.processed_events`.

The persistence capability is exposed through `ProcessedEventStore`, allowing
unit tests to use in-memory or fake implementations while production uses
PostgreSQL.

### Trade-off

Every handled event requires additional database interaction and persistent
idempotency state must be retained and managed.

That cost is accepted because duplicate delivery is a normal possibility in an
at-least-once event-driven system and idempotency must survive service restarts.

### Alternatives considered

-   Track processed IDs only in memory.
-   Assume Kafka consumer-group offsets prevent duplicate delivery.
-   Allow duplicate processing and rely entirely on downstream consumers.

### Consequences

Routing Service can safely receive the same `ShipmentCreated.event_id` more
than once without repeating the route-processing side effect.

This does not prevent Kafka from delivering duplicates and does not by itself
solve reliable downstream publication.

------------------------------------------------------------------------

## ADR-018 --- Use a Transactional Outbox for Routing Service downstream events

**Status:** Accepted\
**Stage:** Routing Service reliability

### Context

Routing Service originally performed two independent operations after
calculating a route:

``` text
1. Publish RouteCalculated to Kafka.
2. Mark ShipmentCreated as processed in PostgreSQL.
```

These operations cannot participate in a shared atomic transaction.

A failure after Kafka accepted `RouteCalculated` but before PostgreSQL recorded
the input event as processed could cause the same `ShipmentCreated` to be
processed again and another downstream event to be published.

Reversing the order would create the opposite risk: PostgreSQL could record the
input event as processed before Kafka publication succeeded, causing the
downstream event to be lost.

### Decision

Use a Transactional Outbox in Routing Service.

Within one PostgreSQL transaction, Routing Service:

``` text
INSERT processed input event
INSERT RouteCalculated outbox event
COMMIT
```

The resulting `RouteCalculated` event is not published directly from
`processShipment`.

A separate outbox publisher later reads unpublished rows from
`routing.outbox_events` and publishes them to Kafka.

### Trade-off

The design introduces additional persistence, an outbox table, a background
publisher, and eventual rather than immediate Kafka publication.

In return, successful route processing and the durable intent to publish its
result are committed atomically in PostgreSQL.

### Alternatives considered

-   Publish to Kafka and then mark the input event as processed.
-   Mark the input event as processed and then publish to Kafka.
-   Attempt compensation after a partial failure.
-   Use distributed transactions across PostgreSQL and Kafka.

### Consequences

A committed Routing Service operation cannot lose its `RouteCalculated` event
solely because Kafka is temporarily unavailable. The event remains durably
pending in the outbox and can be retried later.

The Transactional Outbox does not provide exactly-once delivery. Publication
and marking the outbox row as published still cross the PostgreSQL/Kafka
boundary and therefore have their own failure window.

------------------------------------------------------------------------

## ADR-019 --- Publish Routing outbox events sequentially on a five-second polling interval for the MVP

**Status:** Accepted for MVP\
**Stage:** Routing Service outbox publication

### Context

Once `RouteCalculated` events are stored in the transactional outbox, a
separate mechanism is required to discover pending events and publish them to
Kafka.

The MVP does not currently require high publication throughput or complex
worker coordination. Predictable behavior and easy debugging are more valuable
at this stage.

### Decision

Run the Routing Service outbox publisher independently from the Kafka consumer
loop.

For the current MVP, the publisher polls PostgreSQL every five seconds and
processes pending outbox events sequentially.

For each event it:

``` text
read pending outbox event
publish RouteCalculated to Kafka
mark the outbox event as published
```

The event is marked as published only after Kafka publication reports success.

### Trade-off

Polling introduces publication latency of up to approximately one polling
interval and repeated database queries even when no events are pending.

Sequential publication is easier to reason about and debug but does not make
full use of batching or parallel throughput.

### Planned evolution

If throughput or latency requirements justify it, evaluate shorter or adaptive
polling, batching, parallel workers, notifications, or a dedicated outbox
publisher process while preserving the same reliability semantics.

### Consequences

Outbox publication progresses independently of new `ShipmentCreated` messages.
A pending event can therefore be published after a restart even when the Kafka
consumer loop receives no new work.

------------------------------------------------------------------------

## ADR-020 --- Accept at-least-once outbox publication and require idempotent consumers

**Status:** Accepted\
**Stage:** Routing Service outbox publication

### Context

Even with a Transactional Outbox, Kafka publication and updating
`outbox_events.published_at` are separate operations across two systems.

The following failure remains possible:

``` text
Kafka publish                  SUCCESS
Mark outbox event published   FAILURE
```

In that case the event has reached Kafka but still appears pending in
PostgreSQL. A later retry can publish the same outbox event again.

### Decision

Accept at-least-once publication semantics for outbox events.

Retries reuse the same persisted event, including the same `event_id`, rather
than generating a new event identity.

Downstream consumers must use `event_id`-based idempotency so repeated delivery
of the same event does not repeat business side effects.

For the MVP, preventing event loss is prioritized over attempting to guarantee
exactly-once delivery across PostgreSQL and Kafka.

### Trade-off

Duplicate Kafka records are possible around failures between successful
publication and `published_at` persistence.

Consumers therefore carry additional responsibility and persistent idempotency
state is required where duplicate side effects would be unsafe.

### Alternatives considered

-   Mark an outbox event as published before sending it to Kafka, which could
    lose the event if publication then fails.
-   Attempt to provide exactly-once behavior through application-level timing
    assumptions.
-   Introduce stronger distributed transaction infrastructure for the MVP.

### Consequences

The system prefers possible duplicate delivery to silent event loss.

Routing Service already applies persistent idempotency to `ShipmentCreated`.
Prediction Service now applies the same principle to `RouteCalculated` events,
using persistent `event_id`-based idempotency to tolerate repeated delivery.

------------------------------------------------------------------------

## ADR-021 --- Persist Prediction Service consumer idempotency in PostgreSQL

**Status:** Accepted\
**Stage:** Prediction Service reliability

### Context

Routing Service publishes `RouteCalculated` through an at-least-once
transactional outbox. Around failures, the same persisted event may therefore
be delivered to Prediction Service more than once with the same `event_id`.

If Prediction recalculated the ETA for every delivery, each duplicate input
could create a new `ETAPredicted` event with a different identity and repeat
downstream side effects.

An in-memory processed-event set would not survive service restarts.

### Decision

Persist processed `RouteCalculated.event_id` values in PostgreSQL.

Prediction Service checks whether an input event has already been processed
before running ETA prediction. Successfully processed input IDs are stored in
`prediction.processed_events`.

The persistence capability is exposed through `ProcessedEventStore`, allowing
unit tests to use fakes while production uses `PostgresPredictionStore`.

### Trade-off

Each handled event requires PostgreSQL access and persistent idempotency state
must be retained and managed.

That cost is accepted because duplicate delivery is an expected possibility
under the platform's at-least-once publication semantics.

### Alternatives considered

-   Track processed IDs only in memory.
-   Assume Kafka consumer-group offsets prevent duplicate delivery.
-   Recalculate the ETA for duplicate inputs and rely on downstream consumers.

### Consequences

Prediction Service can receive the same `RouteCalculated.event_id` more than
once without recalculating the ETA or creating another downstream event.

This makes Prediction a safe consumer of Routing's at-least-once outbox
publication, but does not by itself guarantee reliable publication of
`ETAPredicted`.

------------------------------------------------------------------------

## ADR-022 --- Use a Transactional Outbox for Prediction Service downstream events

**Status:** Accepted\
**Stage:** Prediction Service reliability

### Context

After calculating an ETA, Prediction Service needs to record the consumed
`RouteCalculated` event as processed and publish `ETAPredicted` to Kafka.

Persisting the processed input and publishing directly to Kafka would cross the
PostgreSQL/Kafka boundary and recreate the same partial-failure problem already
observed in Routing Service.

### Decision

Use a Transactional Outbox in Prediction Service.

Within one PostgreSQL transaction, Prediction Service:

``` text
INSERT processed RouteCalculated event
INSERT ETAPredicted outbox event
COMMIT
```

The resulting `ETAPredicted` is not published directly from
`handle_route_calculated`.

A separate outbox publisher reads pending rows from
`prediction.outbox_events`, publishes them to Kafka, and marks each row as
published only after successful delivery.

For the current MVP, the publisher runs independently from the Kafka consumer,
polls every five seconds, and processes pending events sequentially.

### Trade-off

The design adds PostgreSQL persistence, an outbox table, a background publisher,
and eventual rather than immediate Kafka publication.

Polling also introduces up to approximately one polling interval of latency and
does not optimize for high throughput.

### Alternatives considered

-   Publish `ETAPredicted` directly and then mark the input as processed.
-   Mark the input as processed before publishing `ETAPredicted`.
-   Attempt compensation after a partial failure.
-   Use distributed transactions across PostgreSQL and Kafka.

### Consequences

Successfully processed `RouteCalculated` events cannot lose their corresponding
`ETAPredicted` solely because Kafka is temporarily unavailable. The outgoing
event remains durably pending and can be retried later.

Publication remains at-least-once. If Kafka publication succeeds but persisting
`published_at` fails, the same persisted `ETAPredicted.event_id` may be
published again on a later retry.

Downstream consumers must therefore tolerate duplicate delivery by `event_id`.

PostgreSQL integration tests verify atomic processed-event/outbox persistence,
rollback on partial failure, pending-event retrieval, and publication-state
updates.

------------------------------------------------------------------------

## ADR-023 --- Commit Routing Kafka offsets only after successful processing

**Status:** Accepted\
**Stage:** Routing Service consumer reliability

### Context

Routing Service consumes `ShipmentCreated` through the `routing-service`
consumer group.

Using Kafka's automatic offset-commit behavior can allow the consumer group's
progress to advance before application processing has completed successfully.

That creates an unsafe failure window:

``` text
Kafka delivers ShipmentCreated
offset committed
PostgreSQL processing fails
```

In that case Kafka may consider the message consumed even though Routing Service
did not durably record the processed input event or create its corresponding
`RouteCalculated` outbox event.

The opposite order creates a safer failure mode:

``` text
Kafka delivers ShipmentCreated
PostgreSQL processing succeeds
offset commit fails
```

Kafka may redeliver the same message, but Routing Service already persists
processed `event_id` values and can safely detect that the event has already
been handled.

### Decision

Use explicit Kafka offset commits in Routing Service.

Routing Service fetches messages without automatically committing their offsets.

For a `ShipmentCreated` event:

``` text
fetch message
process event
commit processed event + RouteCalculated outbox event in PostgreSQL
commit Kafka offset
```

The consumed Kafka offset is committed only after application processing has
completed successfully and the PostgreSQL transaction has committed.

If processing fails, the offset is not committed.

If the offset commit fails after PostgreSQL processing succeeds, the message may
be redelivered. Persistent `event_id`-based idempotency makes that redelivery
safe and prevents a second `RouteCalculated` event from being created.

Events on the shared topic that Routing Service deliberately does not handle are
committed after they are identified as irrelevant to Routing.

The offset-commit capability is abstracted behind `MessageCommitter`, allowing
the consumer-processing semantics to be tested without a running Kafka broker.

### Trade-off

Manual offset management adds explicit control flow and additional failure cases
that the application must reason about.

A successfully processed event can still be redelivered if the Kafka offset
commit fails afterwards.

That duplicate delivery is intentionally accepted because Routing Service
already provides persistent consumer idempotency.

This decision does not yet define retries, backoff, dead-letter handling, or a
general poison-message policy.

### Alternatives considered

-   Continue using automatic offset commits.
-   Commit the offset before application processing.
-   Commit offsets only periodically in batches without tying them to successful
    processing.
-   Treat duplicate delivery as an error instead of relying on consumer
    idempotency.

### Consequences

Routing Service now prefers possible redelivery over silently losing an
unprocessed message.

The main failure windows become:

``` text
processing fails
→ no offset commit
→ Kafka may redeliver

processing succeeds
→ offset commit fails
→ Kafka may redeliver
→ persistent idempotency prevents duplicate business effects
```

Unit tests verify that:

-   ignored event types are committed;
-   successfully processed `ShipmentCreated` events are committed;
-   failed processing does not commit the offset;
-   a commit failure after successful processing can be retried safely without
    generating another outbox event.

The happy path and failed-processing redelivery behavior were also verified
manually with Kafka and PostgreSQL running.

------------------------------------------------------------------------

## ADR-024 --- Commit Prediction Kafka offsets only after successful processing

**Status:** Accepted\
**Stage:** Prediction Service consumer reliability

### Context

Prediction Service consumes `RouteCalculated` through the `prediction-service`
consumer group.

As with Routing Service, allowing Kafka offsets to advance independently from
successful application processing creates an unsafe failure window:

``` text
Kafka delivers RouteCalculated
offset committed
PostgreSQL processing fails
```

In that case Kafka may consider the message consumed even though Prediction
Service did not durably record the processed input event or create its
corresponding `ETAPredicted` outbox event.

Prediction Service already provides persistent `event_id`-based idempotency and
a transactional outbox, so the safer ordering is to complete durable processing
before committing the consumed Kafka offset.

### Decision

Disable Kafka automatic offset commits in Prediction Service and commit offsets
explicitly.

For a `RouteCalculated` event:

``` text
fetch message
validate event
process event
commit processed event + ETAPredicted outbox event in PostgreSQL
commit Kafka offset
```

The consumed Kafka offset is committed only after application processing has
completed successfully and the PostgreSQL transaction has committed.

If decoding, validation, or processing fails, the offset is not committed.

If the offset commit fails after PostgreSQL processing succeeds, Kafka may
redeliver the same message. Persistent `event_id`-based idempotency makes that
redelivery safe and prevents a second `ETAPredicted` outbox event from being
created.

Events on the shared topic that Prediction Service deliberately does not handle
are committed after they are identified as irrelevant to Prediction.

Consumer-message handling is separated into `handle_kafka_message`, allowing
offset-commit behavior to be tested independently from the long-running consumer
loop.

### Trade-off

Manual offset management introduces additional control flow and failure cases.

A successfully processed `RouteCalculated` event can still be redelivered if
the Kafka offset commit fails afterwards.

That duplicate delivery is intentionally accepted because Prediction Service
already provides persistent consumer idempotency.

Malformed or invalid `RouteCalculated` events are deliberately not committed
with the current behavior. Without retry limits or a dead-letter strategy, such
messages can therefore be redelivered indefinitely.

### Alternatives considered

-   Continue using automatic offset commits.
-   Commit the offset before application processing.
-   Commit invalid events immediately and discard them permanently.
-   Commit offsets periodically without tying them to successful processing.
-   Treat duplicate delivery as an error instead of relying on persistent
    idempotency.

### Consequences

Prediction Service now prefers possible redelivery over silently losing an
unprocessed `RouteCalculated` event.

The main failure windows become:

``` text
processing fails
→ no offset commit
→ Kafka may redeliver

processing succeeds
→ offset commit fails
→ Kafka may redeliver
→ persistent idempotency prevents duplicate business effects
```

Unit tests verify that:

-   ignored event types are committed;
-   successfully processed `RouteCalculated` events are committed;
-   failed processing does not commit the offset;
-   a commit failure after successful processing can be retried safely without
    generating another outbox event.

The happy path and failed-processing redelivery behavior were also verified
manually with Kafka and PostgreSQL running.

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

## TD-003 --- Prediction Service consumer idempotency

**Status:** Resolved\
**Area:** Event processing\
**Priority:** High

**Original limitation:**\
Prediction Service did not persist which `RouteCalculated` events it had
processed. If the same input event was delivered more than once, Prediction
could process it again and create another `ETAPredicted` event.

This was especially relevant because Routing's transactional outbox deliberately
uses at-least-once publication semantics and may republish the same
`RouteCalculated.event_id` around failures.

**Resolution:**\
Prediction Service now persists processed `RouteCalculated.event_id` values in
PostgreSQL and ignores duplicate deliveries across restarts. Processing the input
event and persisting the resulting `ETAPredicted` outbox event are performed
atomically in the same PostgreSQL transaction.

**Related decisions:** ADR-017, ADR-020, ADR-021, ADR-022

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

## TD-007 --- Infrastructure integration test coverage is incomplete

**Area:** Testing\
**Priority:** Medium

**Current limitation:**\
Routing Service and Prediction Service now have PostgreSQL integration tests for
outbox persistence and retrieval behavior, while unit tests continue to isolate
infrastructure through interfaces and fakes.

Integration coverage is still incomplete across the platform. In particular,
real Kafka boundaries and other PostgreSQL-backed behavior are not yet covered
systematically by dedicated integration tests.

**Planned evolution:**\
Expand dedicated integration tests incrementally around infrastructure
boundaries as their failure semantics become important.

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

## TD-009 --- Consumer retry and dead-letter semantics are not defined

**Area:** Event processing reliability\
**Priority:** High

**Current limitation:**\
Routing Service and Prediction Service now explicitly commit Kafka offsets only
after successful processing and durable PostgreSQL persistence.

Failed processing therefore leaves the offset uncommitted and allows Kafka to
redeliver the message.

The platform does not yet define retry limits, backoff, dead-letter handling, or
a general poison-message policy. A permanently invalid or repeatedly failing
message can therefore be redelivered indefinitely.

**Planned evolution:**\
Define retry, backoff, and dead-letter behavior that distinguishes transient
processing failures from permanently invalid messages while preserving the
current at-least-once and idempotency guarantees.

**Related decisions:** ADR-023, ADR-024

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

Retries, backoff, and dead-letter handling will be designed as consumer
failure and redelivery semantics are made explicit across the platform.

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
