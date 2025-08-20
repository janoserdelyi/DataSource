# Test Database Containers

This directory contains the setup for running PostgreSQL, Microsoft SQL Server, and MariaDB containers for testing the DataSource project using Podman.

## Prerequisites

- [Podman](https://podman.io/getting-started/installation) installed and running
- [podman-compose](https://github.com/containers/podman-compose) installed

### Installing Podman on Windows

1. Download and install Podman Desktop from: https://podman-desktop.io/
2. Or install via Chocolatey: `choco install podman-desktop`
3. Or install via winget: `winget install RedHat.Podman-Desktop`

### Installing Podman on Linux

Use the automated installer script:
```bash
./scripts/install-prerequisites-linux.sh
```

Or install manually based on your distribution:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install podman
```

**Fedora:**
```bash
sudo dnf install podman
```

**RHEL/CentOS:**
```bash
sudo yum install podman
```

**Arch Linux:**
```bash
sudo pacman -S podman
```

### Installing podman-compose

```bash
pip install podman-compose
```

## Quick Start

1. **Start all containers:**
   ```bash
   # Using PowerShell script (Windows)
   .\scripts\manage-containers.ps1 -Action start
   
   # Using bash script (Linux/macOS)
   ./scripts/manage-containers.sh start
   
   # Using batch script (Windows)
   .\scripts\manage-containers.bat start
   
   # Or using podman-compose directly
   podman-compose up -d
   ```

2. **Use the container configuration in your tests:**
   - Copy `UnitTests\DataSource.config.containers` to `UnitTests\DataSource.config`
   - Or modify your existing configuration to use the container connection strings

3. **Run your unit tests:**
   ```bash
   dotnet test UnitTests\
   ```

## Container Details

### PostgreSQL
- **Image**: `postgres:latest`
- **Container Name**: `datasource-postgres`
- **Port**: `5432`
- **Database**: `testdb`
- **Username**: `testuser`
- **Password**: `testpassword123`

### Microsoft SQL Server
- **Image**: `mcr.microsoft.com/mssql/server:2022-latest`
- **Container Name**: `datasource-sqlserver`
- **Port**: `1433`
- **Database**: `testdb`
- **Username**: `testuser` (and `sa`)
- **Password**: `TestPassword123!`

### MariaDB
- **Image**: `mariadb:latest`
- **Container Name**: `datasource-mariadb`
- **Port**: `3306`
- **Database**: `testdb`
- **Username**: `testuser` (and `root`)
- **Password**: `testpassword123` (root: `rootpassword123`)

## Management Scripts

### PowerShell Script (`scripts\manage-containers.ps1`) - Windows

```powershell
# Start all containers
.\scripts\manage-containers.ps1 -Action start

# Stop all containers
.\scripts\manage-containers.ps1 -Action stop

# Restart all containers
.\scripts\manage-containers.ps1 -Action restart

# Show container status
.\scripts\manage-containers.ps1 -Action status

# Show logs
.\scripts\manage-containers.ps1 -Action logs

# Show logs for specific service
.\scripts\manage-containers.ps1 -Action logs -Service postgres

# Clean up (removes containers and volumes)
# Reset containers (stop, remove, and start fresh)
./scripts/manage-containers.sh reset
```

### Test Setup Scripts

**PowerShell (`scripts\test-setup.ps1`) - Windows:**
```powershell
# Test containers and connectivity
.\scripts\test-setup.ps1

# Test with custom timeout
.\scripts\test-setup.ps1 -TimeoutSeconds 60

# Start containers but skip connectivity tests
.\scripts\test-setup.ps1 -SkipTests
```

**Bash (`scripts/test-setup.sh`) - Linux/macOS:**
```bash
# Test containers and connectivity
./scripts/test-setup.sh

# Test with custom timeout
./scripts/test-setup.sh --timeout 60

# Start containers but skip connectivity tests
./scripts/test-setup.sh --skip-tests
```

### Batch Script (`scripts\manage-containers.bat`) - Windows
```

### Batch Script (`scripts\manage-containers.bat`) - Windows

```powershell
# Start all containers
.\scripts\manage-containers.ps1 -Action start

# Stop all containers
.\scripts\manage-containers.ps1 -Action stop

# Restart all containers
.\scripts\manage-containers.ps1 -Action restart

# Show container status
.\scripts\manage-containers.ps1 -Action status

# Show logs
.\scripts\manage-containers.ps1 -Action logs

# Show logs for specific service
.\scripts\manage-containers.ps1 -Action logs -Service postgres

# Clean up (removes containers and volumes)
.\scripts\manage-containers.ps1 -Action cleanup

# Reset containers (stop, remove, and start fresh)
.\scripts\manage-containers.ps1 -Action reset
```

### Batch Script (`scripts\manage-containers.bat`)

```batch
REM Start all containers
.\scripts\manage-containers.bat start

REM Stop all containers
.\scripts\manage-containers.bat stop

REM Show container status
.\scripts\manage-containers.bat status

REM Show logs
.\scripts\manage-containers.bat logs

REM Clean up (removes containers and volumes)
.\scripts\manage-containers.bat cleanup
```

## Manual Container Management

If you prefer to use podman-compose directly:

```bash
# Start containers
podman-compose up -d

# Stop containers
podman-compose stop

# View logs
podman-compose logs

# Remove containers and volumes
podman-compose down -v

# Check container status
podman-compose ps
```

## Connecting to Databases

### Using command line tools:

**PostgreSQL:**
```bash
podman exec -it datasource-postgres psql -U testuser -d testdb
```

**SQL Server:**
```bash
podman exec -it datasource-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P TestPassword123!
```

**MariaDB:**
```bash
podman exec -it datasource-mariadb mysql -u testuser -p testdb
```

## Configuration Files

- `docker-compose.yml`: Container orchestration configuration
- `UnitTests\DataSource.config.containers`: DataSource configuration for containers
- `scripts\postgres-init.sql`: PostgreSQL initialization script
- `scripts\sqlserver-init.sql`: SQL Server initialization script  
- `scripts\mariadb-init.sql`: MariaDB initialization script

## Troubleshooting

### Containers won't start
1. Check if Podman is running: `podman version`
2. Check for port conflicts: `netstat -an | findstr "5432\|1433\|3306"`
3. View container logs: `podman-compose logs [service-name]`

### Connection issues
1. Verify containers are running: `podman-compose ps`
2. Test connectivity: `telnet localhost 5432` (or appropriate port)
3. Check firewall settings

### Data persistence
- Database data is stored in named volumes
- Use `podman volume ls` to see volumes
- Use `podman volume rm [volume-name]` to remove specific volumes

### Clean slate
To start completely fresh:
```bash
.\scripts\manage-containers.ps1 -Action cleanup
# or
podman-compose down -v
podman volume prune
podman system prune
```

## Security Notes

⚠️ **Important**: These containers are configured for development and testing only. The passwords and configurations used are NOT suitable for production environments.

- Change all default passwords before using in any non-development environment
- Enable SSL/TLS encryption for production use
- Implement proper network segmentation
- Use secrets management for production deployments

## Data Volumes

The containers use named volumes for data persistence:
- `datasource_postgres_data`: PostgreSQL data
- `datasource_sqlserver_data`: SQL Server data  
- `datasource_mariadb_data`: MariaDB data

Data will persist between container restarts unless volumes are explicitly removed.

## Network

All containers run on a custom network named `datasource-network` which allows them to communicate with each other using container names as hostnames.
