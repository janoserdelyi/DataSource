# Scripts Directory

This directory contains management scripts for the DataSource test database containers.

## Scripts Overview

### Container Management Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `manage-containers.ps1` | Windows | PowerShell script for managing containers |
| `manage-containers.sh` | Linux/macOS | Bash script for managing containers |
| `manage-containers.bat` | Windows | Batch script for managing containers |

### Setup and Testing Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `test-setup.ps1` | Windows | PowerShell script to test container setup |
| `test-setup.sh` | Linux/macOS | Bash script to test container setup |
| `install-prerequisites-linux.sh` | Linux | Automated prerequisite installation |

### Database Initialization Scripts

| Script | Database | Description |
|--------|----------|-------------|
| `postgres-init.sql` | PostgreSQL | Initialization script for PostgreSQL container |
| `sqlserver-init.sql` | SQL Server | Initialization script for SQL Server container |
| `mariadb-init.sql` | MariaDB | Initialization script for MariaDB container |

## Usage Examples

### Windows (PowerShell)
```powershell
# Start all containers
.\scripts\manage-containers.ps1 -Action start

# Test setup with connectivity check
.\scripts\test-setup.ps1

# View logs for specific service
.\scripts\manage-containers.ps1 -Action logs -Service postgres
```

### Linux/macOS (Bash)
```bash
# Install prerequisites (Linux only)
./scripts/install-prerequisites-linux.sh

# Start all containers
./scripts/manage-containers.sh start

# Test setup with connectivity check
./scripts/test-setup.sh

# View logs for specific service
./scripts/manage-containers.sh logs postgres
```

### Windows (Batch)
```batch
# Start all containers
.\scripts\manage-containers.bat start

# Stop all containers
.\scripts\manage-containers.bat stop

# View container status
.\scripts\manage-containers.bat status
```

## Common Actions

### Start Everything
```bash
# Windows
.\scripts\test-setup.ps1

# Linux/macOS
./scripts/test-setup.sh
```

### Stop Everything
```bash
# Windows
.\scripts\manage-containers.ps1 -Action stop

# Linux/macOS
./scripts/manage-containers.sh stop
```

### Clean Reset (Remove all data)
```bash
# Windows
.\scripts\manage-containers.ps1 -Action cleanup

# Linux/macOS
./scripts/manage-containers.sh cleanup
```

## Script Permissions

The shell scripts should be executable. If needed, run:
```bash
chmod +x scripts/*.sh
```

## Prerequisites

- **Podman**: Container runtime
- **podman-compose**: Docker Compose compatibility for Podman
- **PowerShell** (Windows): For PowerShell scripts
- **Bash** (Linux/macOS): For shell scripts

## Troubleshooting

### Script won't run (Linux/macOS)
```bash
chmod +x scripts/script-name.sh
```

### Container ports in use
Stop the conflicting services or change ports in `docker-compose.yml`.

### Permission denied (Linux)
Make sure your user is in the correct groups:
```bash
sudo usermod -aG podman $USER
```

## Support

For detailed documentation, see:
- [`CONTAINERS.md`](../CONTAINERS.md) - Comprehensive container documentation
- [`SETUP.md`](../SETUP.md) - Quick setup guide
