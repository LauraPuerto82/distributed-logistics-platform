<#
.SYNOPSIS
Validates the Distributed Logistics Platform end to end.

.DESCRIPTION
Creates a shipment through Order Service and waits for the asynchronous
event flow to produce RouteCalculated and ETAPredicted events in the
Routing and Prediction transactional outboxes.

The local environment must already be running through deploy-local.ps1.

.PREREQUISITES
- PowerShell
- Docker
- The local platform running via deploy-local.ps1
#>

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$OrderServiceUrl = "http://localhost:8080"
$PostgresContainer = "distributed-logistics-postgres"

$MaxAttempts = 30
$RetryDelaySeconds = 1

Write-Host "Checking local deployment..."

try {
    Invoke-RestMethod `
        -Uri "$OrderServiceUrl/health" `
        -Method Get `
        -TimeoutSec 5 |
        Out-Null
}
catch {
    throw "Local deployment is not available. Run .\scripts\deploy-local.ps1 before executing the end-to-end test."
}

Write-Host "Local deployment is available."

Write-Host "Creating test shipment..."

$body = @{
    origin = "Madrid"
    destination = "Barcelona"
    weight = 60
    priority = "HIGH"
} | ConvertTo-Json

$response = Invoke-RestMethod `
    -Method Post `
    -Uri "$OrderServiceUrl/shipments" `
    -ContentType "application/json" `
    -Body $body

$shipmentId = $response.id

if (-not $shipmentId) {
    throw "Shipment creation did not return an ID."
}

Write-Host "Shipment created: $shipmentId"

Write-Host "Waiting for RouteCalculated..."

$routeCalculated = $false

for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
    $routingResult = docker exec $PostgresContainer psql `
        -U logistics `
        -d logistics `
        -t `
        -A `
        -c "SELECT event_type FROM routing.outbox_events WHERE payload->>'shipment_id' = '$shipmentId' AND event_type = 'RouteCalculated' AND published_at IS NOT NULL LIMIT 1;"

    if ($routingResult -match "RouteCalculated") {
        $routeCalculated = $true
        break
    }

    Write-Host "RouteCalculated not available yet ($attempt/$MaxAttempts)..."
    Start-Sleep -Seconds $RetryDelaySeconds
}

if (-not $routeCalculated) {
    throw "RouteCalculated was not published within the expected time."
}

Write-Host "RouteCalculated received."

Write-Host "Waiting for ETAPredicted..."

$etaPredicted = $false

for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
    $predictionResult = docker exec $PostgresContainer psql `
        -U logistics `
        -d logistics `
        -t `
        -A `
        -c "SELECT event_type FROM prediction.outbox_events WHERE payload->>'shipment_id' = '$shipmentId' AND event_type = 'ETAPredicted' AND published_at IS NOT NULL LIMIT 1;"

    if ($predictionResult -match "ETAPredicted") {
        $etaPredicted = $true
        break
    }

    Write-Host "ETAPredicted not available yet ($attempt/$MaxAttempts)..."
    Start-Sleep -Seconds $RetryDelaySeconds
}

if (-not $etaPredicted) {
    throw "ETAPredicted was not published within the expected time."
}

Write-Host "ETAPredicted received."
Write-Host "End-to-end validation passed."