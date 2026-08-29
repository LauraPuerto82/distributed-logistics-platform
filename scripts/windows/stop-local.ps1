<#
.SYNOPSIS
Stops the local Distributed Logistics Platform environment.

.DESCRIPTION
Stops all running application tasks in the local MiniStack ECS cluster,
verifies that no application tasks remain running, and then stops the
PostgreSQL, Kafka, and MiniStack containers managed by Docker Compose.

The original AWS environment variables are restored when the script
finishes, including when an error occurs.

.PREREQUISITES
- PowerShell
- Docker with Docker Compose
- AWS CLI
#>

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$OriginalAwsAccessKeyId = $env:AWS_ACCESS_KEY_ID
$OriginalAwsSecretAccessKey = $env:AWS_SECRET_ACCESS_KEY
$OriginalAwsDefaultRegion = $env:AWS_DEFAULT_REGION

$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "eu-west-1"

try {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

    $MiniStackEndpoint = "http://localhost:4566"
    $DockerComposeFile = Join-Path $RepoRoot "infrastructure/docker/docker-compose.yml"
    $EcsClusterName = "logistics-cluster"

    $MiniStackAvailable = $true

    try {
        aws sts get-caller-identity `
            --endpoint-url $MiniStackEndpoint `
            --output json 2>$null | Out-Null
    }
    catch {
        $MiniStackAvailable = $false
    }

    if ($MiniStackAvailable) {
        Write-Host "Stopping ECS application tasks..."

        $runningTasksResponse = aws ecs list-tasks `
            --cluster $EcsClusterName `
            --desired-status RUNNING `
            --endpoint-url $MiniStackEndpoint `
            --output json | ConvertFrom-Json

        foreach ($taskArn in $runningTasksResponse.taskArns) {
            Write-Host "Stopping task '$taskArn'..."

            aws ecs stop-task `
                --cluster $EcsClusterName `
                --task $taskArn `
                --reason "Local environment stopped" `
                --endpoint-url $MiniStackEndpoint `
                --output json | Out-Null
        }

        Write-Host "Verifying ECS tasks are stopped..."

        $remainingTasks = aws ecs list-tasks `
            --cluster $EcsClusterName `
            --desired-status RUNNING `
            --endpoint-url $MiniStackEndpoint `
            --output json | ConvertFrom-Json

        if ($remainingTasks.taskArns.Count -gt 0) {
            throw "Some ECS application tasks are still running."
        }

        Write-Host "ECS application tasks stopped."
    }
    else {
        Write-Host "MiniStack is not running. Skipping ECS task cleanup."
    }

    Write-Host "Stopping local infrastructure..."

    docker compose `
        -f $DockerComposeFile `
        stop postgres kafka ministack

    Write-Host "Local environment stopped."
}
finally {
    $env:AWS_ACCESS_KEY_ID = $OriginalAwsAccessKeyId
    $env:AWS_SECRET_ACCESS_KEY = $OriginalAwsSecretAccessKey
    $env:AWS_DEFAULT_REGION = $OriginalAwsDefaultRegion
}
