#!/usr/bin/env bash
set -euo pipefail

# Completely removes the local Distributed Logistics Platform environment.
#
# Removes:
# - Running ECS application tasks
# - Docker Compose containers
# - Project volumes
# - Project network, when no containers remain attached
# - Project application images
# - Local ECR-tagged application images
#
# Does not use Docker prune or remove shared Docker resources.
#
# Prerequisites:
# - Bash 4+
# - Docker with Docker Compose
# - AWS CLI

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
DOCKER_COMPOSE_FILE="$REPO_ROOT/infrastructure/docker/docker-compose.yml"
ECS_CLUSTER_NAME="logistics-cluster"
DOCKER_NETWORK="distributed-logistics-network"

PROJECT_IMAGES=(
    "distributed-logistics-order-service:latest"
    "distributed-logistics-routing-service:latest"
    "distributed-logistics-prediction-service:latest"
    "localhost:4566/distributed-logistics-order-service:latest"
    "localhost:4566/distributed-logistics-routing-service:latest"
    "localhost:4566/distributed-logistics-prediction-service:latest"
)

printf '%s\n' "Cleaning local deployment..."

if aws sts get-caller-identity \
    --endpoint-url "$MINISTACK_ENDPOINT" \
    --output json >/dev/null 2>&1; then

    printf '%s\n' "Stopping ECS application tasks..."

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

        printf "Stopping task '%s'...\n" "$task_arn"

        aws ecs stop-task \
            --cluster "$ECS_CLUSTER_NAME" \
            --task "$task_arn" \
            --reason "Local deployment cleanup" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --output json >/dev/null
    done

    printf '%s\n' "ECS application tasks stopped."
else
    printf '%s\n' \
        "MiniStack is not available. Skipping ECS task cleanup."
fi

printf '%s\n' "Removing Docker Compose resources..."

docker compose \
    -f "$DOCKER_COMPOSE_FILE" \
    down \
    --volumes \
    --remove-orphans

printf '%s\n' "Docker Compose resources removed."

printf "Checking project network '%s'...\n" "$DOCKER_NETWORK"

if docker network inspect "$DOCKER_NETWORK" >/dev/null 2>&1; then
    attached_container_count="$(
        docker network inspect \
            --format '{{len .Containers}}' \
            "$DOCKER_NETWORK"
    )"

    if [[ "$attached_container_count" == "0" ]]; then
        printf "Removing project network '%s'...\n" \
            "$DOCKER_NETWORK"

        docker network rm "$DOCKER_NETWORK" >/dev/null

        printf '%s\n' "Project network removed."
    else
        printf "Project network '%s' still has %s attached container(s). Refusing to remove it.\n" \
            "$DOCKER_NETWORK" \
            "$attached_container_count" >&2
        exit 1
    fi
else
    printf "Project network '%s' does not exist.\n" \
        "$DOCKER_NETWORK"
fi

printf '%s\n' "Removing project application images..."

for image in "${PROJECT_IMAGES[@]}"; do
    if docker image inspect "$image" >/dev/null 2>&1; then
        printf "Removing image '%s'...\n" "$image"
        docker image rm "$image" >/dev/null
    else
        printf "Image '%s' does not exist. Skipping.\n" "$image"
    fi
done

printf '%s\n' "Project application images removed."

printf '%s\n' "Local deployment cleaned."
