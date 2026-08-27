<#
.SYNOPSIS
Builds and deploys the Distributed Logistics Platform to a local
MiniStack-based ECS environment.

.DESCRIPTION
Starts PostgreSQL, Kafka, and MiniStack with Docker Compose, builds the
application images, pushes them to the locally emulated ECR registry,
registers Fargate-compatible ECS task definitions, replaces any previous
application tasks, and verifies that Order Service becomes healthy.

MiniStack provides the local AWS-compatible control plane while Docker
provides the underlying container runtime.

The deployment uses explicit ECS tasks rather than relying on ECS Service
reconciliation because that behavior is not fully reproduced by MiniStack.

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
    $RepoRoot = Split-Path -Parent $PSScriptRoot

    $MiniStackEndpoint = "http://localhost:4566"
    # MiniStack's repositoryUri mimics AWS's ECR hostname format, but that
    # hostname is not the local Docker registry endpoint. Docker pushes to
    # the MiniStack gateway instead.
    $MiniStackRegistry = ([System.Uri]$MiniStackEndpoint).Authority
    $DockerComposeFile = Join-Path $RepoRoot "infrastructure/docker/docker-compose.yml"
    $EcsClusterName = "logistics-cluster"

    $DatabaseSecrets = @(
        @{
            Service = "order-service"
            Name = "logistics/order-service/database-url"
            Value = "postgres://logistics:logistics@postgres:5432/logistics"
        },
        @{
            Service = "routing-service"
            Name = "logistics/routing-service/database-url"
            Value = "postgres://logistics:logistics@postgres:5432/logistics"
        },
        @{
            Service = "prediction-service"
            Name = "logistics/prediction-service/database-url"
            Value = "postgresql://logistics:logistics@postgres:5432/logistics"
        }
    )

    Write-Host "Starting local infrastructure..."

    docker compose `
        -f $DockerComposeFile `
        up -d postgres kafka ministack

    Write-Host "Waiting for MiniStack to become ready..."

    $MiniStackMaxAttempts = 15
    $MiniStackRetryDelaySeconds = 2
    $MiniStackReady = $false

    for ($attempt = 1; $attempt -le $MiniStackMaxAttempts; $attempt++) {
        try {
            aws sts get-caller-identity `
                --endpoint-url $MiniStackEndpoint `
                --output json | Out-Null

            $MiniStackReady = $true
            break
        }
        catch {
            Write-Host "MiniStack not ready yet ($attempt/$MiniStackMaxAttempts)..."
        }

        Start-Sleep -Seconds $MiniStackRetryDelaySeconds
    }

    if (-not $MiniStackReady) {
        throw "MiniStack did not become ready within the expected time."
    }

    Write-Host "MiniStack is ready."

    Write-Host "Checking ECS cluster..."

    $clusterResponse = aws ecs describe-clusters `
        --clusters $EcsClusterName `
        --endpoint-url $MiniStackEndpoint `
        --output json | ConvertFrom-Json

    $clusterExists = $clusterResponse.clusters.Count -gt 0 -and `
                    $clusterResponse.clusters[0].status -eq "ACTIVE"

    if ($clusterExists) {
        Write-Host "ECS cluster '$EcsClusterName' already exists."
    }
    else {
        Write-Host "Creating ECS cluster '$EcsClusterName'..."

        aws ecs create-cluster `
            --cluster-name $EcsClusterName `
            --endpoint-url $MiniStackEndpoint `
            --output json | Out-Null

        Write-Host "ECS cluster created."
    }

    $EcrRepositories = @(
        "logistics-order-service",
        "logistics-routing-service",
        "logistics-prediction-service"
    )

    Write-Host "Checking ECR repositories..."

    foreach ($repository in $EcrRepositories) {
        $repositoryExists = $true

        try {
            aws ecr describe-repositories `
                --repository-names $repository `
                --endpoint-url $MiniStackEndpoint `
                --output json | Out-Null
        }
        catch {
            $repositoryExists = $false
        }

        if ($repositoryExists) {
            Write-Host "ECR repository '$repository' already exists."
        }
        else {
            Write-Host "Creating ECR repository '$repository'..."

            aws ecr create-repository `
                --repository-name $repository `
                --endpoint-url $MiniStackEndpoint `
                --output json | Out-Null

            Write-Host "ECR repository '$repository' created."
        }
    }

    $Services = @(
        "order-service",
        "routing-service",
        "prediction-service"
    )

    Write-Host "Configuring application secrets..."

    $DatabaseSecretArns = @{}

    foreach ($secret in $DatabaseSecrets) {
        $secretExists = $true

        try {
            $secretResponse = aws secretsmanager describe-secret `
                --secret-id $secret.Name `
                --endpoint-url $MiniStackEndpoint `
                --output json | ConvertFrom-Json
        }
        catch {
            $secretExists = $false
        }

        if ($secretExists) {
            Write-Host "Updating secret '$($secret.Name)'..."

            aws secretsmanager put-secret-value `
                --secret-id $secret.Name `
                --secret-string $secret.Value `
                --endpoint-url $MiniStackEndpoint `
                --output json | Out-Null
        }
        else {
            Write-Host "Creating secret '$($secret.Name)'..."

            $secretResponse = aws secretsmanager create-secret `
                --name $secret.Name `
                --secret-string $secret.Value `
                --endpoint-url $MiniStackEndpoint `
                --output json | ConvertFrom-Json
        }

        $DatabaseSecretArns[$secret.Service] = $secretResponse.ARN
    }

    Write-Host "Application secrets configured."

    Write-Host "Building application images..."

    docker compose `
        -f $DockerComposeFile `
        build $Services

    Write-Host "Tagging and pushing images to local ECR..."

    foreach ($service in $Services) {
        $imageName = "logistics-$service"
        $localEcrImage = "$MiniStackRegistry/${imageName}:latest"

        Write-Host "Processing '$imageName'..."

        docker tag `
            "${imageName}:latest" `
            $localEcrImage

        docker push $localEcrImage
    }

    Write-Host "Application images are available in local ECR."

    $TaskDefinitions = @(
        @{
            Name = "order-service"
            Path = Join-Path $RepoRoot "infrastructure/ministack/ecs/order-task-definition.template.json"
        },
        @{
            Name = "routing-service"
            Path = Join-Path $RepoRoot "infrastructure/ministack/ecs/routing-task-definition.template.json"
        },
        @{
            Name = "prediction-service"
            Path = Join-Path $RepoRoot "infrastructure/ministack/ecs/prediction-task-definition.template.json"
        }
    )

    $RegisteredTaskDefinitions = @{}

    Write-Host "Registering ECS task definitions..."

    foreach ($taskDefinition in $TaskDefinitions) {
        Write-Host "Registering '$($taskDefinition.Path)'..."

        # Task definition templates contain placeholders for environment-specific
        # resource ARNs that are resolved at deployment time.
        $taskDefinitionContent = Get-Content `
            -Path $taskDefinition.Path `
            -Raw

        $databaseSecretArn = $DatabaseSecretArns[$taskDefinition.Name]

        $taskDefinitionContent = $taskDefinitionContent.Replace(
            "__DATABASE_URL_SECRET_ARN__",
            $databaseSecretArn
        )

        $tempTaskDefinitionPath = Join-Path `
            $env:TEMP `
            "$($taskDefinition.Name)-task-definition.json"

        Set-Content `
            -Path $tempTaskDefinitionPath `
            -Value $taskDefinitionContent `
            -Encoding utf8

        $taskDefinitionPath = $tempTaskDefinitionPath.Replace("\", "/")

        $response = aws ecs register-task-definition `
            --cli-input-json "file://$taskDefinitionPath" `
            --endpoint-url $MiniStackEndpoint `
            --output json | ConvertFrom-Json

        $RegisteredTaskDefinitions[$taskDefinition.Name] = `
            $response.taskDefinition.taskDefinitionArn
    }

    Write-Host "ECS task definitions registered."

    Write-Host "Registered task definitions for this deployment:"

    foreach ($service in $RegisteredTaskDefinitions.Keys) {
        Write-Host "$service -> $($RegisteredTaskDefinitions[$service])"
    }

    # MiniStack does not currently reproduce ECS Service task reconciliation
    # reliably, so the local deployment replaces tasks explicitly with RunTask.
    Write-Host "Stopping previous ECS application tasks..."

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
            --reason "Replaced by local deployment" `
            --endpoint-url $MiniStackEndpoint `
            --output json | Out-Null
    }

    Write-Host "Previous ECS application tasks stopped."

    Write-Host "Starting ECS application tasks..."

    $StartedTasks = @{}

    foreach ($service in $RegisteredTaskDefinitions.Keys) {
        $taskDefinitionArn = $RegisteredTaskDefinitions[$service]

        Write-Host "Starting '$service'..."

        $response = aws ecs run-task `
            --cluster $EcsClusterName `
            --task-definition $taskDefinitionArn `
            --launch-type FARGATE `
            --endpoint-url $MiniStackEndpoint `
            --output json | ConvertFrom-Json

        if ($response.failures.Count -gt 0) {
            throw "Failed to start ECS task for '$service'."
        }

        if ($response.tasks.Count -eq 0) {
            throw "No ECS task was created for '$service'."
        }

        $StartedTasks[$service] = $response.tasks[0].taskArn

        Write-Host "'$service' started."
    }

    Write-Host "ECS application tasks started."

    Write-Host "Waiting for Order Service to become healthy..."

    $HealthUrl = "http://localhost:8080/health"
    $MaxAttempts = 15
    $RetryDelaySeconds = 2
    $healthy = $false

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $response = Invoke-RestMethod `
                -Uri $HealthUrl `
                -Method Get `
                -TimeoutSec 2

            if ($response.status -eq "ok") {
                $healthy = $true
                break
            }
        }
        catch {
            Write-Host "Order Service not ready yet ($attempt/$MaxAttempts)..."
        }

        Start-Sleep -Seconds $RetryDelaySeconds
    }

    if (-not $healthy) {
        throw "Order Service did not become healthy within the expected time."
    }

    Write-Host "Order Service is healthy."

    Write-Host "Waiting for Kafka consumers to become ready..."

    $ConsumerGroups = @(
        "routing-service",
        "prediction-service"
    )

    $KafkaConsumerMaxAttempts = 20
    $KafkaConsumerRetryDelaySeconds = 2

    foreach ($group in $ConsumerGroups) {
        $consumerReady = $false

        for ($attempt = 1; $attempt -le $KafkaConsumerMaxAttempts; $attempt++) {
            try {
                $groupState = docker exec logistics-kafka `
                    /opt/kafka/bin/kafka-consumer-groups.sh `
                    --bootstrap-server localhost:9092 `
                    --describe `
                    --group $group `
                    --state 2>&1

                $stableGroup = $groupState |
                    Where-Object {
                        $_ -match "^$group\s+" -and
                        $_ -match "\sStable\s+" -and
                        $_ -match "\s[1-9]\d*$"
                    }

                if ($stableGroup) {
                    $consumerReady = $true
                    break
                }
            }
            catch {
                # A missing or not-yet-active consumer group is expected
                # while the service is still joining Kafka.
            }

            Write-Host "Kafka consumer group '$group' not ready yet ($attempt/$KafkaConsumerMaxAttempts)..."

            Start-Sleep -Seconds $KafkaConsumerRetryDelaySeconds
        }

        if (-not $consumerReady) {
            throw "Kafka consumer group '$group' did not become stable within the expected time."
        }

        Write-Host "Kafka consumer group '$group' is ready."
    }

    Write-Host "Local deployment is ready."
}
finally {
    $env:AWS_ACCESS_KEY_ID = $OriginalAwsAccessKeyId
    $env:AWS_SECRET_ACCESS_KEY = $OriginalAwsSecretAccessKey
    $env:AWS_DEFAULT_REGION = $OriginalAwsDefaultRegion
}
