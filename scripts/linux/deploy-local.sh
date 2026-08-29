#!/usr/bin/env bash
set -euo pipefail

# Builds and deploys the Distributed Logistics Platform to a local
# MiniStack-based ECS environment.
#
# Prerequisites:
# - Bash 4+
# - Docker with Docker Compose
# - AWS CLI
# - curl

ORIGINAL_AWS_ACCESS_KEY_ID_SET=${AWS_ACCESS_KEY_ID+x}
ORIGINAL_AWS_SECRET_ACCESS_KEY_SET=${AWS_SECRET_ACCESS_KEY+x}
ORIGINAL_AWS_DEFAULT_REGION_SET=${AWS_DEFAULT_REGION+x}
ORIGINAL_AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-}
ORIGINAL_AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-}
ORIGINAL_AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION-}

restore_aws_environment() {
    if [[ -n "$ORIGINAL_AWS_ACCESS_KEY_ID_SET" ]]; then
        export AWS_ACCESS_KEY_ID="$ORIGINAL_AWS_ACCESS_KEY_ID"
    else
        unset AWS_ACCESS_KEY_ID
    fi

    if [[ -n "$ORIGINAL_AWS_SECRET_ACCESS_KEY_SET" ]]; then
        export AWS_SECRET_ACCESS_KEY="$ORIGINAL_AWS_SECRET_ACCESS_KEY"
    else
        unset AWS_SECRET_ACCESS_KEY
    fi

    if [[ -n "$ORIGINAL_AWS_DEFAULT_REGION_SET" ]]; then
        export AWS_DEFAULT_REGION="$ORIGINAL_AWS_DEFAULT_REGION"
    else
        unset AWS_DEFAULT_REGION
    fi
}

trap restore_aws_environment EXIT

export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="eu-west-1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MINISTACK_ENDPOINT="http://localhost:4566"
MINISTACK_REGISTRY="localhost:4566"
DOCKER_COMPOSE_FILE="$REPO_ROOT/infrastructure/docker/docker-compose.yml"
ECS_CLUSTER_NAME="logistics-cluster"
KAFKA_CONTAINER="distributed-logistics-kafka"
DOCKER_NETWORK="distributed-logistics-network"

SERVICES=(
    "order-service"
    "routing-service"
    "prediction-service"
)

ECR_REPOSITORIES=(
    "distributed-logistics-order-service"
    "distributed-logistics-routing-service"
    "distributed-logistics-prediction-service"
)

KAFKA_TOPICS=(
    "shipment-events"
    "routing-service-dlq"
    "prediction-service-dlq"
)

CONSUMER_GROUPS=(
    "routing-service"
    "prediction-service"
)

declare -A DATABASE_SECRET_NAMES=(
    ["order-service"]="logistics/order-service/database-url"
    ["routing-service"]="logistics/routing-service/database-url"
    ["prediction-service"]="logistics/prediction-service/database-url"
)

declare -A DATABASE_SECRET_VALUES=(
    ["order-service"]="postgres://logistics:logistics@postgres:5432/logistics"
    ["routing-service"]="postgres://logistics:logistics@postgres:5432/logistics"
    ["prediction-service"]="postgresql://logistics:logistics@postgres:5432/logistics"
)

declare -A TASK_DEFINITION_PATHS=(
    ["order-service"]="$REPO_ROOT/infrastructure/ministack/ecs/order-task-definition.template.json"
    ["routing-service"]="$REPO_ROOT/infrastructure/ministack/ecs/routing-task-definition.template.json"
    ["prediction-service"]="$REPO_ROOT/infrastructure/ministack/ecs/prediction-task-definition.template.json"
)

declare -A DATABASE_SECRET_ARNS
declare -A REGISTERED_TASK_DEFINITIONS

printf '%s\n' "Starting local infrastructure..."

docker compose \
    -f "$DOCKER_COMPOSE_FILE" \
    up -d postgres kafka ministack

printf '%s\n' "Waiting for PostgreSQL to become ready..."

POSTGRES_MAX_ATTEMPTS=15
POSTGRES_RETRY_DELAY_SECONDS=2
POSTGRES_READY=false

for ((attempt = 1; attempt <= POSTGRES_MAX_ATTEMPTS; attempt++)); do
    if docker compose \
        -f "$DOCKER_COMPOSE_FILE" \
        exec -T postgres \
        pg_isready \
        -U logistics \
        -d logistics >/dev/null 2>&1; then

        POSTGRES_READY=true
        break
    fi

    printf 'PostgreSQL not ready yet (%d/%d)...\n' \
        "$attempt" \
        "$POSTGRES_MAX_ATTEMPTS"

    sleep "$POSTGRES_RETRY_DELAY_SECONDS"
done

if [[ "$POSTGRES_READY" != true ]]; then
    printf '%s\n' \
        "PostgreSQL did not become ready within the expected time." >&2
    exit 1
fi

printf '%s\n' "PostgreSQL is ready."

printf '%s\n' "Applying database migrations..."

MIGRATIONS_DIRECTORY="$REPO_ROOT/infrastructure/postgres/migrations"

docker run --rm \
    --network "$DOCKER_NETWORK" \
    -v "$MIGRATIONS_DIRECTORY:/db/migrations" \
    -e DATABASE_URL="postgres://logistics:logistics@postgres:5432/logistics?sslmode=disable" \
    ghcr.io/amacneil/dbmate:2.35.1 \
    --migrations-dir /db/migrations \
    up

printf '%s\n' "Database migrations applied."

printf '%s\n' "Ensuring Kafka topics exist..."

for topic in "${KAFKA_TOPICS[@]}"; do
    docker exec "$KAFKA_CONTAINER" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor 1
done

printf '%s\n' "Kafka topics are ready."

printf '%s\n' "Waiting for MiniStack to become ready..."

MINISTACK_MAX_ATTEMPTS=15
MINISTACK_RETRY_DELAY_SECONDS=2
MINISTACK_READY=false

for ((attempt = 1; attempt <= MINISTACK_MAX_ATTEMPTS; attempt++)); do
    if aws sts get-caller-identity \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --output json >/dev/null 2>&1; then

        MINISTACK_READY=true
        break
    fi

    printf 'MiniStack not ready yet (%d/%d)...\n' \
        "$attempt" \
        "$MINISTACK_MAX_ATTEMPTS"

    sleep "$MINISTACK_RETRY_DELAY_SECONDS"
done

if [[ "$MINISTACK_READY" != true ]]; then
    printf '%s\n' \
        "MiniStack did not become ready within the expected time." >&2
    exit 1
fi

printf '%s\n' "MiniStack is ready."

printf '%s\n' "Checking ECS cluster..."

CLUSTER_STATUS="$(
    aws ecs describe-clusters \
        --clusters "$ECS_CLUSTER_NAME" \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --query 'clusters[0].status' \
        --output text 2>/dev/null || true
)"

if [[ "$CLUSTER_STATUS" == "ACTIVE" ]]; then
    printf "ECS cluster '%s' already exists.\n" \
        "$ECS_CLUSTER_NAME"
else
    printf "Creating ECS cluster '%s'...\n" \
        "$ECS_CLUSTER_NAME"

    aws ecs create-cluster \
        --cluster-name "$ECS_CLUSTER_NAME" \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --output json >/dev/null

    printf '%s\n' "ECS cluster created."
fi

printf '%s\n' "Checking ECR repositories..."

for repository in "${ECR_REPOSITORIES[@]}"; do
    if aws ecr describe-repositories \
        --repository-names "$repository" \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --output json >/dev/null 2>&1; then

        printf "ECR repository '%s' already exists.\n" \
            "$repository"
    else
        printf "Creating ECR repository '%s'...\n" \
            "$repository"

        aws ecr create-repository \
            --repository-name "$repository" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --output json >/dev/null

        printf "ECR repository '%s' created.\n" \
            "$repository"
    fi
done

printf '%s\n' "Configuring application secrets..."

for service in "${SERVICES[@]}"; do
    secret_name="${DATABASE_SECRET_NAMES[$service]}"
    secret_value="${DATABASE_SECRET_VALUES[$service]}"

    if aws secretsmanager describe-secret \
        --secret-id "$secret_name" \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --output json >/dev/null 2>&1; then

        printf "Updating secret '%s'...\n" \
            "$secret_name"

        aws secretsmanager put-secret-value \
            --secret-id "$secret_name" \
            --secret-string "$secret_value" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --output json >/dev/null
    else
        printf "Creating secret '%s'...\n" \
            "$secret_name"

        aws secretsmanager create-secret \
            --name "$secret_name" \
            --secret-string "$secret_value" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --output json >/dev/null
    fi

    DATABASE_SECRET_ARNS[$service]="$(
        aws secretsmanager describe-secret \
            --secret-id "$secret_name" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --query 'ARN' \
            --output text
    )"
done

printf '%s\n' "Application secrets configured."

printf '%s\n' "Building application images..."

docker compose \
    -f "$DOCKER_COMPOSE_FILE" \
    build "${SERVICES[@]}"

printf '%s\n' "Tagging and pushing images to local ECR..."

for service in "${SERVICES[@]}"; do
    image_name="distributed-logistics-$service"
    local_ecr_image="$MINISTACK_REGISTRY/${image_name}:latest"

    printf "Processing '%s'...\n" "$image_name"

    docker tag \
        "${image_name}:latest" \
        "$local_ecr_image"

    docker push "$local_ecr_image"
done

printf '%s\n' "Application images are available in local ECR."

printf '%s\n' "Registering ECS task definitions..."

for service in "${SERVICES[@]}"; do
    template_path="${TASK_DEFINITION_PATHS[$service]}"

    printf "Registering '%s'...\n" \
        "$template_path"

    temp_task_definition_path="$(
        mktemp "/tmp/${service}-task-definition.XXXXXX.json"
    )"

    sed \
        "s|__DATABASE_URL_SECRET_ARN__|${DATABASE_SECRET_ARNS[$service]}|g" \
        "$template_path" \
        > "$temp_task_definition_path"

    REGISTERED_TASK_DEFINITIONS[$service]="$(
        aws ecs register-task-definition \
            --cli-input-json "file://$temp_task_definition_path" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --query 'taskDefinition.taskDefinitionArn' \
            --output text
    )"

    rm -f "$temp_task_definition_path"
done

printf '%s\n' "ECS task definitions registered."

printf '%s\n' "Registered task definitions for this deployment:"

for service in "${SERVICES[@]}"; do
    printf '%s -> %s\n' \
        "$service" \
        "${REGISTERED_TASK_DEFINITIONS[$service]}"
done

# MiniStack does not currently reproduce ECS Service task reconciliation
# reliably, so the local deployment replaces tasks explicitly with RunTask.
printf '%s\n' "Stopping previous ECS application tasks..."

mapfile -t running_tasks < <(
    aws ecs list-tasks \
        --cluster "$ECS_CLUSTER_NAME" \
        --desired-status RUNNING \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --query 'taskArns[]' \
        --output text 2>/dev/null |
        tr '\t' '\n'
)

for task_arn in "${running_tasks[@]}"; do
    if [[ -z "$task_arn" || "$task_arn" == "None" ]]; then
        continue
    fi

    printf "Stopping task '%s'...\n" \
        "$task_arn"

    aws ecs stop-task \
        --cluster "$ECS_CLUSTER_NAME" \
        --task "$task_arn" \
        --reason "Replaced by local deployment" \
        --endpoint-url "$MINISTACK_ENDPOINT" \
        --output json >/dev/null
done

printf '%s\n' "Previous ECS application tasks stopped."

printf '%s\n' "Starting ECS application tasks..."

for service in "${SERVICES[@]}"; do
    task_definition_arn="${REGISTERED_TASK_DEFINITIONS[$service]}"

    printf "Starting '%s'...\n" \
        "$service"

    read -r failure_count task_count task_arn < <(
        aws ecs run-task \
            --cluster "$ECS_CLUSTER_NAME" \
            --task-definition "$task_definition_arn" \
            --launch-type FARGATE \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --query '[length(failures), length(tasks), tasks[0].taskArn]' \
            --output text
    )

    if [[ "$failure_count" != "0" ]]; then
        printf "Failed to start ECS task for '%s'.\n" \
            "$service" >&2
        exit 1
    fi

    if [[ "$task_count" == "0" ||
          -z "$task_arn" ||
          "$task_arn" == "None" ]]; then

        printf "No ECS task was created for '%s'.\n" \
            "$service" >&2
        exit 1
    fi

    printf "'%s' started.\n" \
        "$service"
done

printf '%s\n' "ECS application tasks started."

printf '%s\n' "Waiting for Order Service to become healthy..."

HEALTH_URL="http://localhost:8080/health"
MAX_ATTEMPTS=15
RETRY_DELAY_SECONDS=2
HEALTHY=false

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    health_response="$(
        curl \
            --silent \
            --show-error \
            --max-time 2 \
            "$HEALTH_URL" 2>/dev/null || true
    )"

    if [[ "$health_response" == *'"status":"ok"'* ||
          "$health_response" == *'"status": "ok"'* ]]; then

        HEALTHY=true
        break
    fi

    printf 'Order Service not ready yet (%d/%d)...\n' \
        "$attempt" \
        "$MAX_ATTEMPTS"

    sleep "$RETRY_DELAY_SECONDS"
done

if [[ "$HEALTHY" != true ]]; then
    printf '%s\n' \
        "Order Service did not become healthy within the expected time." >&2
    exit 1
fi

printf '%s\n' "Order Service is healthy."

printf '%s\n' "Waiting for Kafka consumers to become ready..."

KAFKA_CONSUMER_MAX_ATTEMPTS=20
KAFKA_CONSUMER_RETRY_DELAY_SECONDS=2

for group in "${CONSUMER_GROUPS[@]}"; do
    consumer_ready=false

    for ((attempt = 1; attempt <= KAFKA_CONSUMER_MAX_ATTEMPTS; attempt++)); do
        group_state="$(
            docker exec "$KAFKA_CONTAINER" \
                /opt/kafka/bin/kafka-consumer-groups.sh \
                --bootstrap-server localhost:9092 \
                --describe \
                --group "$group" \
                --state 2>&1 || true
        )"

        if printf '%s\n' "$group_state" |
            awk -v group="$group" '
                $1 == group &&
                $0 ~ /Stable/ &&
                $NF ~ /^[1-9][0-9]*$/ {
                    found = 1
                }

                END {
                    exit(found ? 0 : 1)
                }
            '; then

            group_members="$(
                docker exec "$KAFKA_CONTAINER" \
                    /opt/kafka/bin/kafka-consumer-groups.sh \
                    --bootstrap-server localhost:9092 \
                    --describe \
                    --group "$group" \
                    --members 2>&1 || true
            )"

            if printf '%s\n' "$group_members" |
                awk -v group="$group" '
                    $1 == group &&
                    $NF ~ /^[1-9][0-9]*$/ {
                        found = 1
                    }

                    END {
                        exit(found ? 0 : 1)
                    }
                '; then

                consumer_ready=true
                break
            fi
        fi

        printf "Kafka consumer group '%s' not ready yet (%d/%d)...\n" \
            "$group" \
            "$attempt" \
            "$KAFKA_CONSUMER_MAX_ATTEMPTS"

        sleep "$KAFKA_CONSUMER_RETRY_DELAY_SECONDS"
    done

    if [[ "$consumer_ready" != true ]]; then
        printf "Kafka consumer group '%s' did not become ready with assigned partitions within the expected time.\n" \
            "$group" >&2
        exit 1
    fi

    printf "Kafka consumer group '%s' is ready.\n" \
        "$group"
done

printf '%s\n' "Local deployment is ready."
