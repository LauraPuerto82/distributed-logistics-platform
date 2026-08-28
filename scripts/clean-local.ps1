<#
.SYNOPSIS
    Completely removes the local Distributed Logistics Platform environment.

.DESCRIPTION
    Stops running ECS application tasks in MiniStack, removes Docker Compose
    containers, project volumes, and the project network, then removes only
    Docker image tags explicitly owned by this project.

    Shared/base images are preserved. No Docker prune commands are used.
#>

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$OriginalAwsAccessKeyId = $env:AWS_ACCESS_KEY_ID
$OriginalAwsSecretAccessKey = $env:AWS_SECRET_ACCESS_KEY
$OriginalAwsDefaultRegion = $env:AWS_DEFAULT_REGION
$OriginalAwsRegion = $env:AWS_REGION

try {
    $env:AWS_ACCESS_KEY_ID = "test"
    $env:AWS_SECRET_ACCESS_KEY = "test"
    $env:AWS_DEFAULT_REGION = "eu-west-1"
    $env:AWS_REGION = "eu-west-1"

    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $RepoRoot = Split-Path -Parent $ScriptDir

    $DockerComposeFile = Join-Path `
        $RepoRoot `
        "infrastructure/docker/docker-compose.yml"

    $MiniStackEndpoint = "http://localhost:4566"
    $EcsClusterName = "logistics-cluster"
    $ProjectNetwork = "distributed-logistics-network"

    $ProjectImages = @(
        "distributed-logistics-order-service:latest",
        "distributed-logistics-routing-service:latest",
        "distributed-logistics-prediction-service:latest",
        "localhost:4566/distributed-logistics-order-service:latest",
        "localhost:4566/distributed-logistics-routing-service:latest",
        "localhost:4566/distributed-logistics-prediction-service:latest"
    )

    Write-Host "Cleaning local Distributed Logistics Platform environment..."

    #
    # 1. Stop ECS tasks while MiniStack is still available.
    #
    $MiniStackAvailable = $true

    try {
        aws sts get-caller-identity `
            --endpoint-url $MiniStackEndpoint `
            --no-cli-pager `
            *> $null
    }
    catch {
        $MiniStackAvailable = $false
    }

    if ($MiniStackAvailable) {
        Write-Host "Stopping ECS application tasks..."

        $taskArns = aws ecs list-tasks `
            --cluster $EcsClusterName `
            --endpoint-url $MiniStackEndpoint `
            --query "taskArns[]" `
            --output text `
            --no-cli-pager

        if ($LASTEXITCODE -ne 0) {
            throw "Failed to list ECS tasks."
        }

        if ($taskArns) {
            foreach ($taskArn in ($taskArns -split "\s+")) {
                if (-not [string]::IsNullOrWhiteSpace($taskArn)) {
                    Write-Host "Stopping task '$taskArn'..."

                    aws ecs stop-task `
                        --cluster $EcsClusterName `
                        --task $taskArn `
                        --endpoint-url $MiniStackEndpoint `
                        --no-cli-pager `
                        *> $null
                }
            }

            Write-Host "ECS application tasks stopped."
        }
        else {
            Write-Host "No running ECS application tasks found."
        }
    }
    else {
        Write-Host "MiniStack is not running. Skipping ECS task cleanup."
    }

    #
    # 2. Remove only Compose-managed resources for this project.
    #
    Write-Host "Removing Docker Compose containers, volumes, and network..."

    docker compose `
        -f $DockerComposeFile `
        down `
        --volumes `
        --remove-orphans

    #
    # 3. Docker may occasionally leave the explicitly named network behind
    #    briefly. Remove it only if it still exists and has no containers.
    #
    $networkExists = docker network ls `
        --filter "name=^${ProjectNetwork}$" `
        --format "{{.Name}}"

    if ($networkExists -eq $ProjectNetwork) {
        $networkContainers = docker network inspect `
            $ProjectNetwork `
            --format "{{len .Containers}}"

        if ($networkContainers -eq "0") {
            Write-Host "Removing remaining project network '$ProjectNetwork'..."
            docker network rm $ProjectNetwork *> $null
        }
        else {
            throw "Project network '$ProjectNetwork' is still in use and was not removed."
        }
    }

    #
    # 4. Remove only exact application image tags owned by this project.
    #
    Write-Host "Removing project application images..."

    $existingImages = docker image ls `
        --format "{{.Repository}}:{{.Tag}}"

    foreach ($image in $ProjectImages) {
        if ($existingImages -contains $image) {
            Write-Host "Removing image '$image'..."
            docker image rm $image *> $null
        }
    }

    Write-Host "Local environment cleaned."
}
finally {
    $env:AWS_ACCESS_KEY_ID = $OriginalAwsAccessKeyId
    $env:AWS_SECRET_ACCESS_KEY = $OriginalAwsSecretAccessKey
    $env:AWS_DEFAULT_REGION = $OriginalAwsDefaultRegion
    $env:AWS_REGION = $OriginalAwsRegion
}