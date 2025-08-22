#!/usr/bin/env pwsh

<#
.SYNOPSIS
	Manage test database containers for DataSource project
.DESCRIPTION
	This script helps manage Podman containers for PostgreSQL, SQL Server, and MariaDB
	used for testing the DataSource project.
.PARAMETER Action
	The action to perform: start, stop, restart, status, logs, cleanup
.PARAMETER Service
	Optional: Specific service to target (postgres, sqlserver, mariadb)
.EXAMPLE
	.\manage-containers.ps1 -Action start
	.\manage-containers.ps1 -Action stop -Service postgres
	.\manage-containers.ps1 -Action logs -Service sqlserver
#>

param(
	[Parameter(Mandatory=$true)]
	[ValidateSet("start", "stop", "restart", "status", "logs", "cleanup", "reset")]
	[string]$Action,

	[Parameter(Mandatory=$false)]
	[ValidateSet("postgres", "sqlserver", "mariadb")]
	[string]$Service = ""
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"

function Write-Info {
	param([string]$Message)
	Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Write-Warning {
	param([string]$Message)
	Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-Error {
	param([string]$Message)
	Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Test-PodmanInstalled {
	try {
	    $null = Get-Command podman -ErrorAction Stop
	    return $true
	}
	catch {
	    Write-Error "Podman is not installed or not in PATH. Please install Podman first."
	    Write-Info "Visit: https://podman.io/getting-started/installation"
	    return $false
	}
}

function Start-Containers {
	Write-Info "Starting database containers..."
	if ($Service) {
	    podman-compose -f $ComposeFile up -d $Service
	} else {
	    podman-compose -f $ComposeFile up -d
	}

	if ($LASTEXITCODE -eq 0) {
	    Write-Info "Containers started successfully!"
	    Show-ConnectionInfo
	} else {
	    Write-Error "Failed to start containers."
	}
}

function Stop-Containers {
	Write-Info "Stopping database containers..."
	if ($Service) {
	    podman-compose -f $ComposeFile stop $Service
	} else {
	    podman-compose -f $ComposeFile stop
	}

	if ($LASTEXITCODE -eq 0) {
	    Write-Info "Containers stopped successfully!"
	} else {
	    Write-Error "Failed to stop containers."
	}
}

function Restart-Containers {
	Write-Info "Restarting database containers..."
	if ($Service) {
	    podman-compose -f $ComposeFile restart $Service
	} else {
	    podman-compose -f $ComposeFile restart
	}

	if ($LASTEXITCODE -eq 0) {
	    Write-Info "Containers restarted successfully!"
	} else {
	    Write-Error "Failed to restart containers."
	}
}

function Show-Status {
	Write-Info "Container status:"
	podman-compose -f $ComposeFile ps
}

function Show-Logs {
	if ($Service) {
	    Write-Info "Showing logs for $Service..."
	    podman-compose -f $ComposeFile logs -f $Service
	} else {
	    Write-Info "Showing logs for all containers..."
	    podman-compose -f $ComposeFile logs -f
	}
}

function Remove-Containers {
	Write-Warning "This will remove all containers and volumes. Data will be lost!"
	$confirmation = Read-Host "Are you sure you want to continue? (yes/no)"

	if ($confirmation -eq "yes") {
	    Write-Info "Removing containers and volumes..."
	    podman-compose -f $ComposeFile down -v

	    if ($LASTEXITCODE -eq 0) {
	        Write-Info "Cleanup completed!"
	    } else {
	        Write-Error "Failed to cleanup containers."
	    }
	} else {
	    Write-Info "Cleanup cancelled."
	}
}

function Reset-Containers {
	Write-Info "Resetting containers (stop, remove, and start fresh)..."
	podman-compose -f $ComposeFile down -v
	podman-compose -f $ComposeFile up -d

	if ($LASTEXITCODE -eq 0) {
	    Write-Info "Containers reset successfully!"
	    Show-ConnectionInfo
	} else {
	    Write-Error "Failed to reset containers."
	}
}

function Show-ConnectionInfo {
	Write-Info "Database connection information:"
	Write-Host ""
	Write-Host "PostgreSQL:" -ForegroundColor Cyan
	Write-Host "  Host: localhost" -ForegroundColor White
	Write-Host "  Port: 5432" -ForegroundColor White
	Write-Host "  Database: testdb" -ForegroundColor White
	Write-Host "  Username: testuser" -ForegroundColor White
	Write-Host "  Password: testpassword123" -ForegroundColor White
	Write-Host ""
	Write-Host "SQL Server:" -ForegroundColor Cyan
	Write-Host "  Host: localhost" -ForegroundColor White
	Write-Host "  Port: 1433" -ForegroundColor White
	Write-Host "  Database: testdb" -ForegroundColor White
	Write-Host "  Username: testuser" -ForegroundColor White
	Write-Host "  Password: TestPassword123!" -ForegroundColor White
	Write-Host "  SA Password: TestPassword123!" -ForegroundColor White
	Write-Host ""
	Write-Host "MariaDB:" -ForegroundColor Cyan
	Write-Host "  Host: localhost" -ForegroundColor White
	Write-Host "  Port: 3306" -ForegroundColor White
	Write-Host "  Database: testdb" -ForegroundColor White
	Write-Host "  Username: testuser" -ForegroundColor White
	Write-Host "  Password: testpassword123" -ForegroundColor White
	Write-Host "  Root Password: rootpassword123" -ForegroundColor White
	Write-Host ""
	Write-Info "Use 'DataSource.config.containers' for your unit tests configuration."
}

# Main execution
if (-not (Test-PodmanInstalled)) {
	exit 1
}

if (-not (Test-Path $ComposeFile)) {
	Write-Error "docker-compose.yml not found at: $ComposeFile"
	exit 1
}

switch ($Action) {
	"start" { Start-Containers }
	"stop" { Stop-Containers }
	"restart" { Restart-Containers }
	"status" { Show-Status }
	"logs" { Show-Logs }
	"cleanup" { Remove-Containers }
	"reset" { Reset-Containers }
	default {
	    Write-Error "Unknown action: $Action"
	    exit 1
	}
}
