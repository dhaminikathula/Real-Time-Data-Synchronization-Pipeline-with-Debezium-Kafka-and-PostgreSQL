Write-Host "Waiting for Kafka Connect to be ready..." -ForegroundColor Cyan

# Loop until Kafka Connect returns 200 OK
while ($true) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8083/" -UseBasicParsing -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            break
        }
    } catch {
        Write-Host "`tKafka Connect is not ready yet. Retrying in 5 seconds..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
    }
}

Write-Host "Kafka Connect is ready. Registering Debezium connector..." -ForegroundColor Cyan

$body = @{
    name = "products-connector"
    config = @{
        "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
        "database.hostname" = "postgres"
        "database.port" = "5432"
        "database.user" = "user"
        "database.password" = "password"
        "database.dbname" = "products_db"
        "topic.prefix" = "pg-server"
        "decimal.handling.mode" = "double"
        "database.server.name" = "pg-server"
        "table.include.list" = "public.products"
        "plugin.name" = "pgoutput"
        "tombstones.on.delete" = "true"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8083/connectors/" -Method Post -Body $body -ContentType "application/json" -ErrorAction Stop
    $response | ConvertTo-Json -Depth 5 | Write-Host
    Write-Host "`nDebezium connector registered successfully." -ForegroundColor Green
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 409) {
        Write-Host "`nDebezium connector is already registered." -ForegroundColor Green
    } else {
        Write-Host "`nError registering connector: $_" -ForegroundColor Red
    }
}
