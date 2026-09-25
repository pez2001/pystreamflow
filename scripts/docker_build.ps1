# PowerShell script to build and run PyStreamFlow Docker container

param(
    [switch]$BuildOnly,
    [switch]$Compose
)

$ErrorActionPreference = 'Stop'

Write-Host "Building PyStreamFlow Docker image..." -ForegroundColor Cyan
docker build -t pystreamflow:latest .

if ($BuildOnly) {
    Write-Host "Build complete. Skipping run." -ForegroundColor Green
    exit 0
}

if ($Compose) {
    Write-Host "Starting with docker-compose..." -ForegroundColor Cyan
    docker compose up -d
    Write-Host "PyStreamFlow running at http://localhost:8000" -ForegroundColor Green
} else {
    Write-Host "Running container..." -ForegroundColor Cyan
    docker run -d --name pystreamflow -p 8000:8000 -p 8080:8080 -v "$PWD\workflows:/app/workflows" pystreamflow:latest
    Write-Host "PyStreamFlow running at http://localhost:8000" -ForegroundColor Green
}
