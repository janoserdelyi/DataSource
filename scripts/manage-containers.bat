@echo off
REM Batch script to manage test database containers

if "%1"=="" goto usage
if "%1"=="start" goto start
if "%1"=="stop" goto stop
if "%1"=="restart" goto restart
if "%1"=="status" goto status
if "%1"=="logs" goto logs
if "%1"=="cleanup" goto cleanup
if "%1"=="reset" goto reset
goto usage

:start
echo Starting database containers...
podman-compose up -d
if %errorlevel% equ 0 (
	echo Containers started successfully!
	call :show_connection_info
) else (
	echo Failed to start containers.
)
goto end

:stop
echo Stopping database containers...
podman-compose stop
if %errorlevel% equ 0 (
	echo Containers stopped successfully!
) else (
	echo Failed to stop containers.
)
goto end

:restart
echo Restarting database containers...
podman-compose restart
if %errorlevel% equ 0 (
	echo Containers restarted successfully!
) else (
	echo Failed to restart containers.
)
goto end

:status
echo Container status:
podman-compose ps
goto end

:logs
echo Showing container logs...
podman-compose logs
goto end

:cleanup
echo WARNING: This will remove all containers and volumes. Data will be lost!
set /p confirm="Are you sure you want to continue? (yes/no): "
if /i "%confirm%"=="yes" (
	echo Removing containers and volumes...
	podman-compose down -v
	if %errorlevel% equ 0 (
	    echo Cleanup completed!
	) else (
	    echo Failed to cleanup containers.
	)
) else (
	echo Cleanup cancelled.
)
goto end

:reset
echo Resetting containers...
podman-compose down -v
podman-compose up -d
if %errorlevel% equ 0 (
	echo Containers reset successfully!
	call :show_connection_info
) else (
	echo Failed to reset containers.
)
goto end

:show_connection_info
echo.
echo Database connection information:
echo.
echo PostgreSQL:
echo   Host: localhost
echo   Port: 5432
echo   Database: testdb
echo   Username: testuser
echo   Password: testpassword123
echo.
echo SQL Server:
echo   Host: localhost
echo   Port: 1433
echo   Database: testdb
echo   Username: testuser
echo   Password: TestPassword123!
echo.
echo MariaDB:
echo   Host: localhost
echo   Port: 3306
echo   Database: testdb
echo   Username: testuser
echo   Password: testpassword123
echo.
echo Use 'DataSource.config.containers' for your unit tests configuration.
echo.
goto :eof

:usage
echo Usage: %0 [start^|stop^|restart^|status^|logs^|cleanup^|reset]
echo.
echo   start    - Start all database containers
echo   stop     - Stop all database containers
echo   restart  - Restart all database containers
echo   status   - Show container status
echo   logs     - Show container logs
echo   cleanup  - Remove containers and volumes (WARNING: Data loss!)
echo   reset    - Reset containers (stop, remove, and start fresh)
echo.

:end
