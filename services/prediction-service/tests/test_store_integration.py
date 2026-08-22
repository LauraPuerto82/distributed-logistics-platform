import os

import psycopg
import pytest

from prediction_service.models import ETAPredictedEvent, ETAPredictedPayload
from prediction_service.store import PostgresPredictionStore


pytestmark = pytest.mark.integration


def test_mark_processed_with_outbox_event_persists_both_records():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.fail("DATABASE_URL is not set")

    store = PostgresPredictionStore(database_url)

    input_event_id = "evt_prediction_integration_input"

    outbox_event = ETAPredictedEvent(
        event_id="evt_prediction_integration_output",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_prediction_integration",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    try:
        store.mark_processed_with_outbox_event(
            input_event_id,
            outbox_event,
        )

        assert store.is_processed(input_event_id)

        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT payload
                    FROM prediction.outbox_events
                    WHERE event_id = %s
                    """,
                    (outbox_event.event_id,),
                )

                row = cursor.fetchone()

        assert row is not None

        stored_payload = row[0]

        assert stored_payload["event_id"] == outbox_event.event_id
        assert stored_payload["event_type"] == "ETAPredicted"
        assert stored_payload["shipment_id"] == outbox_event.shipment_id
        assert (
            stored_payload["payload"]["estimated_travel_minutes"]
            == outbox_event.payload.estimated_travel_minutes
        )

    finally:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM prediction.outbox_events
                    WHERE event_id = %s
                    """,
                    (outbox_event.event_id,),
                )

                cursor.execute(
                    """
                    DELETE FROM prediction.processed_events
                    WHERE event_id = %s
                    """,
                    (input_event_id,),
                )


def test_mark_processed_with_outbox_event_rolls_back_when_outbox_insert_fails():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.fail("DATABASE_URL is not set")

    store = PostgresPredictionStore(database_url)

    input_event_id = "evt_prediction_rollback_input"

    outbox_event = ETAPredictedEvent(
        event_id="evt_prediction_rollback_output",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_prediction_rollback",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    try:
        # Pre-insert the outbox event so the store's second INSERT
        # fails with a duplicate primary key.
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO prediction.outbox_events (
                        event_id,
                        event_type,
                        payload
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (
                        outbox_event.event_id,
                        outbox_event.event_type,
                        outbox_event.model_dump_json(),
                    ),
                )

        with pytest.raises(psycopg.errors.UniqueViolation):
            store.mark_processed_with_outbox_event(
                input_event_id,
                outbox_event,
            )

        # The first INSERT must also have been rolled back.
        assert not store.is_processed(input_event_id)

    finally:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM prediction.outbox_events
                    WHERE event_id = %s
                    """,
                    (outbox_event.event_id,),
                )

                cursor.execute(
                    """
                    DELETE FROM prediction.processed_events
                    WHERE event_id = %s
                    """,
                    (input_event_id,),
                )


def test_get_pending_events_returns_unpublished_events():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.fail("DATABASE_URL is not set")

    store = PostgresPredictionStore(database_url)

    input_event_id = "evt_prediction_pending_input"

    outbox_event = ETAPredictedEvent(
        event_id="evt_prediction_pending_output",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_prediction_pending",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    try:
        store.mark_processed_with_outbox_event(
            input_event_id,
            outbox_event,
        )

        pending_events = store.get_pending_events()

        matching_events = [
            event for event in pending_events if event.event_id == outbox_event.event_id
        ]

        assert len(matching_events) == 1

        pending_event = matching_events[0]

        assert isinstance(pending_event, ETAPredictedEvent)
        assert pending_event.event_type == "ETAPredicted"
        assert pending_event.shipment_id == "shp_prediction_pending"
        assert pending_event.payload.estimated_travel_minutes == 123

    finally:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM prediction.outbox_events
                    WHERE event_id = %s
                    """,
                    (outbox_event.event_id,),
                )

                cursor.execute(
                    """
                    DELETE FROM prediction.processed_events
                    WHERE event_id = %s
                    """,
                    (input_event_id,),
                )


def test_mark_published_removes_event_from_pending_events():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.fail("DATABASE_URL is not set")

    store = PostgresPredictionStore(database_url)

    input_event_id = "evt_prediction_publish_input"

    outbox_event = ETAPredictedEvent(
        event_id="evt_prediction_publish_output",
        event_type="ETAPredicted",
        timestamp="2026-08-22T09:00:00Z",
        shipment_id="shp_prediction_publish",
        payload=ETAPredictedPayload(
            estimated_travel_minutes=123,
        ),
    )

    try:
        store.mark_processed_with_outbox_event(
            input_event_id,
            outbox_event,
        )

        pending_before = store.get_pending_events()

        assert any(event.event_id == outbox_event.event_id for event in pending_before)

        store.mark_published(outbox_event.event_id)

        pending_after = store.get_pending_events()

        assert not any(
            event.event_id == outbox_event.event_id for event in pending_after
        )

    finally:
        with psycopg.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM prediction.outbox_events
                    WHERE event_id = %s
                    """,
                    (outbox_event.event_id,),
                )

                cursor.execute(
                    """
                    DELETE FROM prediction.processed_events
                    WHERE event_id = %s
                    """,
                    (input_event_id,),
                )
