#!/usr/bin/env bash
set -euo pipefail

# Validates the complete local event-driven flow:
#
# POST /shipments
#   -> ShipmentCreated
#   -> Routing Service
#   -> RouteCalculated
#   -> Prediction Service
#   -> ETAPredicted
#
# Prerequisites:
# - The local platform running via scripts/linux/deploy-local.sh
# - Docker
# - curl

ORDER_SERVICE_URL="http://localhost:8080"
POSTGRES_CONTAINER="distributed-logistics-postgres"

MAX_ATTEMPTS=30
RETRY_DELAY_SECONDS=1

printf '%s\n' "Checking local deployment..."

if ! curl \
    --silent \
    --show-error \
    --fail \
    --max-time 2 \
    "$ORDER_SERVICE_URL/health" >/dev/null 2>&1; then

    printf '%s\n' \
        "Local deployment is not available. Run ./scripts/linux/deploy-local.sh before executing the end-to-end test." >&2
    exit 1
fi

printf '%s\n' "Local deployment is available."

printf '%s\n' "Creating test shipment..."

shipment_response="$(
    curl \
        --silent \
        --show-error \
        --fail \
        --request POST \
        --header "Content-Type: application/json" \
        --data '{
            "origin": "Madrid",
            "destination": "Barcelona",
            "weight": 60,
            "priority": "HIGH"
        }' \
        "$ORDER_SERVICE_URL/shipments"
)"

shipment_id="$(
    printf '%s\n' "$shipment_response" |
        sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
)"

if [[ -z "$shipment_id" ]]; then
    printf '%s\n' \
        "Could not extract the shipment ID from the Order Service response." >&2
    printf 'Response: %s\n' "$shipment_response" >&2
    exit 1
fi

printf "Shipment created: %s\n" "$shipment_id"

printf '%s\n' "Waiting for RouteCalculated..."

route_calculated=false

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    routing_result="$(
        docker exec "$POSTGRES_CONTAINER" \
            psql \
            -U logistics \
            -d logistics \
            -t \
            -A \
            -c "SELECT event_type
                FROM routing.outbox_events
                WHERE payload->>'shipment_id' = '$shipment_id'
                  AND event_type = 'RouteCalculated'
                  AND published_at IS NOT NULL
                LIMIT 1;"
    )"

    if [[ "$routing_result" == *"RouteCalculated"* ]]; then
        route_calculated=true
        break
    fi

    printf 'RouteCalculated not available yet (%d/%d)...\n' \
        "$attempt" \
        "$MAX_ATTEMPTS"

    sleep "$RETRY_DELAY_SECONDS"
done

if [[ "$route_calculated" != true ]]; then
    printf "RouteCalculated was not received for shipment '%s' within the expected time.\n" \
        "$shipment_id" >&2
    exit 1
fi

printf '%s\n' "RouteCalculated received."

printf '%s\n' "Waiting for ETAPredicted..."

eta_predicted=false

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    prediction_result="$(
        docker exec "$POSTGRES_CONTAINER" \
            psql \
            -U logistics \
            -d logistics \
            -t \
            -A \
            -c "SELECT event_type
                FROM prediction.outbox_events
                WHERE payload->>'shipment_id' = '$shipment_id'
                  AND event_type = 'ETAPredicted'
                  AND published_at IS NOT NULL
                LIMIT 1;"
    )"

    if [[ "$prediction_result" == *"ETAPredicted"* ]]; then
        eta_predicted=true
        break
    fi

    printf 'ETAPredicted not available yet (%d/%d)...\n' \
        "$attempt" \
        "$MAX_ATTEMPTS"

    sleep "$RETRY_DELAY_SECONDS"
done

if [[ "$eta_predicted" != true ]]; then
    printf "ETAPredicted was not received for shipment '%s' within the expected time.\n" \
        "$shipment_id" >&2
    exit 1
fi

printf '%s\n' "ETAPredicted received."

printf '%s\n' "End-to-end validation passed."
