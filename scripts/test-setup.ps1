#!/usr/bin/env pwsh

<#
.SYNOPSIS
	Test the container setup for DataSource project
.DESCRIPTION
	This script starts the containers, waits for them to be ready, and runs a basic connectivity test
#>

param(
	[Parameter(Mandatory=$false)]
	[switch]$SkipTests,

	[Parameter(Mandatory=$false)]
	[int]$TimeoutSeconds = 120
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
	    Write-Error "Podman is not installed or not in PATH."
	    return $false
	}
}

function Wait-ForContainer {
	param(
	    [string]$ContainerName,
	    [string]$Host,
	    [int]$Port,
	    [int]$TimeoutSeconds = 60
	)

	Write-Info "Waiting for $ContainerName to be ready on ${Host}:${Port}..."

	$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

	do {
	    try {
	        $connection = New-Object System.Net.Sockets.TcpClient
	        $connection.Connect($Host, $Port)
	        $connection.Close()
	        Write-Info "$ContainerName is ready!"
	        return $true
	    }
	    catch {
	        Start-Sleep -Seconds 2
	    }
	} while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds)

	$stopwatch.Stop()
	Write-Warning "$ContainerName did not become ready within $TimeoutSeconds seconds"
	return $false
}

function Test-DatabaseConnectivity {
	Write-Info "Testing database connectivity..."

	# Test PostgreSQL
	Write-Info "Testing PostgreSQL..."
	try {
	    $result = podman exec -it datasource-postgres psql -U testuser -d testdb -c "SELECT 1;" 2>&1
	    if ($LASTEXITCODE -eq 0) {
	        Write-Info "PostgreSQL: ✓ Connected successfully"
	    } else {
	        Write-Warning "PostgreSQL: ✗ Connection failed"
	    }
	}
	catch {
	    Write-Warning "PostgreSQL: ✗ Connection test failed"
	}

	# Test SQL Server
	Write-Info "Testing SQL Server..."
	try {
	    $result = podman exec -it datasource-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "TestPassword123!" -Q "SELECT 1" 2>&1
	    if ($LASTEXITCODE -eq 0) {
	        Write-Info "SQL Server: ✓ Connected successfully"
	    } else {
	        Write-Warning "SQL Server: ✗ Connection failed"
	    }
	}
	catch {
	    Write-Warning "SQL Server: ✗ Connection test failed"
	}

	# Test MariaDB
	Write-Info "Testing MariaDB..."
	try {
	    $result = podman exec -it datasource-mariadb mysql -u testuser -ptestpassword123 -e "SELECT 1;" 2>&1
	    if ($LASTEXITCODE -eq 0) {
	        Write-Info "MariaDB: ✓ Connected successfully"
	    } else {
	        Write-Warning "MariaDB: ✗ Connection failed"
	    }
	}
	catch {
	    Write-Warning "MariaDB: ✗ Connection test failed"
	}
}

# Main execution
Write-Info "Starting DataSource container test..."

if (-not (Test-PodmanInstalled)) {
	exit 1
}

if (-not (Test-Path $ComposeFile)) {
	Write-Error "docker-compose.yml not found at: $ComposeFile"
	exit 1
}

# Start containers
Write-Info "Starting containers..."
podman-compose -f $ComposeFile up -d

if ($LASTEXITCODE -ne 0) {
	Write-Error "Failed to start containers"
	exit 1
}

# Wait for containers to be ready
$allReady = $true
$allReady = (Wait-ForContainer "PostgreSQL" "localhost" 5432 $TimeoutSeconds) -and $allReady
$allReady = (Wait-ForContainer "SQL Server" "localhost" 1433 $TimeoutSeconds) -and $allReady
$allReady = (Wait-ForContainer "MariaDB" "localhost" 3306 $TimeoutSeconds) -and $allReady

if (-not $allReady) {
	Write-Warning "Some containers may not be fully ready. Check logs with: podman-compose logs"
}

# Test connectivity
if (-not $SkipTests) {
	Start-Sleep -Seconds 5  # Give containers a bit more time
	Test-DatabaseConnectivity
}

Write-Info "Container setup test completed!"
Write-Info "You can now run your unit tests with: dotnet test UnitTests\"
Write-Info "Remember to copy UnitTests\DataSource.config.containers to UnitTests\DataSource.config"
