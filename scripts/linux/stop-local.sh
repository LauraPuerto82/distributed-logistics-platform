#!/usr/bin/env bash
set -euo pipefail

# Stops the local Distributed Logistics Platform while preserving
# containers, volumes and MiniStack state for a later restart.
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

printf '%s\n' "Stopping local deployment..."

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
            --reason "Local deployment stopped" \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --output json >/dev/null
    done

    mapfile -t remaining_tasks < <(
        aws ecs list-tasks \
            --cluster "$ECS_CLUSTER_NAME" \
            --desired-status RUNNING \
            --endpoint-url "$MINISTACK_ENDPOINT" \
            --query 'taskArns[]' \
            --output text 2>/dev/null |
            tr '\t' '\n'
    )

    remaining_task_count=0

    for task_arn in "${remaining_tasks[@]}"; do
        if [[ -n "$task_arn" && "$task_arn" != "None" ]]; then
            ((remaining_task_count += 1))
        fi
    done

    if ((remaining_task_count > 0)); then
        printf '%s\n' \
            "Some ECS application tasks are still running." >&2
        exit 1
    fi

    printf '%s\n' "ECS application tasks stopped."
else
    printf '%s\n' \
        "MiniStack is not available. Skipping ECS task cleanup."
fi

printf '%s\n' "Stopping local infrastructure..."

docker compose \
    -f "$DOCKER_COMPOSE_FILE" \
    stop postgres kafka ministack

printf '%s\n' "Local deployment stopped."
