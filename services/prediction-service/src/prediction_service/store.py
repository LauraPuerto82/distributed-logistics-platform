from typing import Protocol

import psycopg

from prediction_service.models import ETAPredictedEvent


class ProcessedEventStore(Protocol):
    def is_processed(self, event_id: str) -> bool: ...

    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event: ETAPredictedEvent,
    ) -> None: ...


class OutboxStore(Protocol):
    def get_pending_events(self) -> list[ETAPredictedEvent]: ...

    def mark_published(self, event_id: str) -> None: ...


class PostgresPredictionStore:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def is_processed(self, event_id: str) -> bool:
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM prediction.processed_events
                        WHERE event_id = %s
                    )
                    """,
                    (event_id,),
                )

                row = cursor.fetchone()

        return bool(row[0])

    def mark_processed_with_outbox_event(
        self,
        event_id: str,
        outbox_event: ETAPredictedEvent,
    ) -> None:
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                # Persist the processed event and its outgoing event in the same transaction
                # so either both records are committed or neither is.
                cursor.execute(
                    """
                    INSERT INTO prediction.processed_events (event_id)
                    VALUES (%s)
                    """,
                    (event_id,),
                )

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

    def get_pending_events(self) -> list[ETAPredictedEvent]:
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT payload
                    FROM prediction.outbox_events
                    WHERE published_at IS NULL
                    ORDER BY created_at
                    """
                )

                rows = cursor.fetchall()

        return [ETAPredictedEvent.model_validate(row[0]) for row in rows]

    def mark_published(self, event_id: str) -> None:
        with psycopg.connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE prediction.outbox_events
                    SET published_at = NOW()
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )
