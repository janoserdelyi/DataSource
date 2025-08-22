# Setup Guide: Test Databases with Podman

This guide will help you set up PostgreSQL, Microsoft SQL Server, and MariaDB containers for testing your DataSource project.

## Quick Setup

### 1. Install Prerequisites

**Windows:**
- Install [Podman Desktop](https://podman-desktop.io/)
- Install [podman-compose](https://github.com/containers/podman-compose): `pip install podman-compose`

**Linux:**
```bash
# Automated installation
./scripts/install-prerequisites-linux.sh

# Or install manually
sudo apt install podman  # Ubuntu/Debian
pip install podman-compose
```

### 2. Start the Containers
```bash
# Windows PowerShell
.\scripts\manage-containers.ps1 -Action start

# Linux/macOS
./scripts/manage-containers.sh start

# Windows Batch
.\scripts\manage-containers.bat start

# Or using podman-compose directly
podman-compose up -d
```

### 3. Configure Your Tests
Replace your `UnitTests\DataSource.config` with the container configuration:
```bash
# Windows
copy UnitTests\DataSource.config.containers UnitTests\DataSource.config

# Linux/macOS
cp UnitTests/DataSource.config.containers UnitTests/DataSource.config
```

### 4. Run Your Tests
```bash
dotnet test UnitTests\
```

## Container Information

| Database | Port | Database | Username | Password |
|----------|------|----------|----------|----------|
| PostgreSQL | 5432 | testdb | testuser | testpassword123 |
| SQL Server | 1433 | testdb | testuser | TestPassword123! |
| MariaDB | 3306 | testdb | testuser | testpassword123 |

## Management Commands

**Windows PowerShell:**
```powershell
# Start all containers
.\scripts\manage-containers.ps1 -Action start

# Stop all containers
.\scripts\manage-containers.ps1 -Action stop

# Show container status
.\scripts\manage-containers.ps1 -Action status

# Show logs
.\scripts\manage-containers.ps1 -Action logs

# Clean up (removes all data)
.\scripts\manage-containers.ps1 -Action cleanup

# Reset containers (fresh start)
.\scripts\manage-containers.ps1 -Action reset
```

**Linux/macOS Bash:**
```bash
# Start all containers
./scripts/manage-containers.sh start

# Stop all containers
./scripts/manage-containers.sh stop

# Show container status
./scripts/manage-containers.sh status

# Show logs
./scripts/manage-containers.sh logs

# Clean up (removes all data)
./scripts/manage-containers.sh cleanup

# Reset containers (fresh start)
./scripts/manage-containers.sh reset
```

## Testing All Databases

Your unit tests have been updated to test against all three database types:
- **PostgreSQL**: Uses `:parameter` syntax
- **SQL Server**: Uses `@parameter` syntax  
- **MariaDB/MySQL**: Uses `?parameter` syntax

The test fixtures will automatically create test tables in all connected databases and run the same tests against each one.

## Database-Specific Notes

### PostgreSQL
- Uses standard PostgreSQL data types
- Boolean support: `bool`
- Auto-increment: `generated always as identity`

### SQL Server
- Uses SQL Server data types
- Boolean support: `bit`
- Auto-increment: `identity`

### MariaDB/MySQL
- Uses MySQL/MariaDB data types
- Boolean support: `tinyint(1)` (MySQL doesn't have native boolean)
- Auto-increment: `auto_increment`
- Uses `varchar` instead of `nvarchar` (MariaDB handles UTF-8 natively)

## Troubleshooting

### Containers won't start
1. Check Podman: `podman version`
2. Check port conflicts: `netstat -an | findstr "5432\|1433\|3306"`
3. View logs: `podman-compose logs [service-name]`

### Connection issues
1. Verify containers are running: `podman-compose ps`
2. Test connectivity: `telnet localhost [port]`

### For more details
See [CONTAINERS.md](CONTAINERS.md) for comprehensive documentation.

## Security Warning
⚠️ **These containers are for development/testing only!** The passwords and configurations are not suitable for production use.
