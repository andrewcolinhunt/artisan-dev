#!/bin/bash
#
# Initialize PostgreSQL database for Prefect server
#
# This script sets up a local PostgreSQL instance using the pixi-managed
# PostgreSQL installation. The database is stored in the project's data
# directory and doesn't require root privileges.
#
# Usage:
#   ./init_postgres.sh                 # Initialize database (one-time setup)
#   ./init_postgres.sh --reset         # Reset database (deletes all data!)
#   ./init_postgres.sh --update-config # Update config on existing database
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# PostgreSQL data directory (within project, not system-wide)
PGDATA="$PROJECT_ROOT/.prefect-postgres"
PGPORT=5433  # Use non-standard port to avoid conflicts with system PostgreSQL
PGUSER="prefect"
PGDATABASE="prefect"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Write sentinel-bounded custom config block (idempotent)
write_custom_config() {
    local conf="$PGDATA/postgresql.conf"
    local start_marker="# --- artisan custom settings ---"
    local end_marker="# --- end artisan custom settings ---"

    # One-time migration: remove old-format block (pre-sentinel)
    if grep -q "# Custom settings for Prefect" "$conf" 2>/dev/null; then
        sed -i '/# Custom settings for Prefect/,/logging_collector = off/d' "$conf"
    fi

    # Remove existing sentinel-bounded block if present
    if grep -q "$start_marker" "$conf" 2>/dev/null; then
        sed -i "/$start_marker/,/$end_marker/d" "$conf"
    fi

    cat >> "$conf" << EOF
$start_marker
listen_addresses = 'localhost'
port = $PGPORT
max_connections = 200
shared_buffers = 256MB
log_destination = 'stderr'
logging_collector = off
$end_marker
EOF
}

# Check if pixi environment has PostgreSQL
check_postgres_available() {
    if ! command -v "$PROJECT_ROOT/.pixi/envs/default/bin/initdb" &> /dev/null; then
        log_error "PostgreSQL not found in pixi environment"
        echo ""
        echo "Run 'pixi install' first to install PostgreSQL from conda-forge"
        exit 1
    fi
}

# Initialize the database cluster
init_database() {
    log_info "Initializing PostgreSQL database cluster..."

    mkdir -p "$PGDATA"

    # Initialize the database cluster
    "$PROJECT_ROOT/.pixi/envs/default/bin/initdb" \
        -D "$PGDATA" \
        -U "$PGUSER" \
        --auth=trust \
        --encoding=UTF8

    write_custom_config

    log_info "Database cluster initialized at $PGDATA"
}

# Start PostgreSQL temporarily to create database
create_database() {
    log_info "Starting PostgreSQL to create database..."

    # Start PostgreSQL in background
    "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" \
        -D "$PGDATA" \
        -l "$PGDATA/postgres.log" \
        -o "-p $PGPORT" \
        start

    # Wait for PostgreSQL to be ready
    sleep 2

    # Create the prefect database
    "$PROJECT_ROOT/.pixi/envs/default/bin/createdb" \
        -h localhost \
        -p "$PGPORT" \
        -U "$PGUSER" \
        "$PGDATABASE" 2>/dev/null || true

    log_info "Database '$PGDATABASE' created"

    # Stop PostgreSQL (the start script will start it properly)
    "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" \
        -D "$PGDATA" \
        stop

    log_info "PostgreSQL stopped (use start_prefect_server.sh to start)"
}

# Ensure the prefect database exists in an already-initialized cluster.
# Handles the case where a previous init failed after initdb but before createdb.
ensure_database_exists() {
    local pg_was_stopped=false

    # Start PostgreSQL if not running
    if [ ! -f "$PGDATA/postmaster.pid" ] || \
       ! kill -0 "$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null)" 2>/dev/null; then
        pg_was_stopped=true
        "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" \
            -D "$PGDATA" \
            -l "$PGDATA/postgres.log" \
            -o "-p $PGPORT" \
            start -w -t 10 || {
                log_error "Could not start PostgreSQL to verify database"
                return 1
            }
    fi

    # Check if the database exists; create it if missing
    if ! "$PROJECT_ROOT/.pixi/envs/default/bin/psql" \
        -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -c "SELECT 1" &>/dev/null; then
        log_warn "Database '$PGDATABASE' missing — creating it now..."
        "$PROJECT_ROOT/.pixi/envs/default/bin/createdb" \
            -h localhost -p "$PGPORT" -U "$PGUSER" "$PGDATABASE"
        log_info "Database '$PGDATABASE' created"
    fi

    # Stop PostgreSQL again if we started it
    if [ "$pg_was_stopped" = true ]; then
        "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" -D "$PGDATA" stop -w
    fi
}

# Main script
main() {
    check_postgres_available

    # Handle --update-config flag
    if [ "$1" == "--update-config" ]; then
        if [ ! -d "$PGDATA" ] || [ ! -f "$PGDATA/PG_VERSION" ]; then
            log_error "No database to update. Run init_postgres.sh first."
            exit 1
        fi
        write_custom_config
        log_info "Configuration updated. Restart PostgreSQL to apply."
        exit 0
    fi

    # Handle --reset flag
    if [ "$1" == "--reset" ]; then
        log_warn "Resetting PostgreSQL database (this will delete ALL Prefect data)..."
        read -p "Are you sure? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Stop PostgreSQL if running
            if [ -f "$PGDATA/postmaster.pid" ]; then
                "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" -D "$PGDATA" stop 2>/dev/null || true
            fi
            rm -rf "$PGDATA"
            log_info "Database reset complete"
        else
            log_info "Reset cancelled"
            exit 0
        fi
    fi

    # Check if already initialized
    if [ -d "$PGDATA" ] && [ -f "$PGDATA/PG_VERSION" ]; then
        # Cluster exists — verify the prefect database was actually created.
        # A previous init may have failed after initdb but before createdb
        # (e.g., port conflict prevented pg_ctl start).
        ensure_database_exists
        log_info "PostgreSQL database already exists at $PGDATA"
        echo ""
        echo "To reset the database, run:"
        echo "  $0 --reset"
        echo ""
        echo "To start the Prefect server with PostgreSQL, run:"
        echo "  ./start_prefect_server.sh"
        exit 0
    fi

    echo "================================================================"
    echo "Initializing PostgreSQL for Prefect Server"
    echo "================================================================"
    echo ""
    echo "Configuration:"
    echo "  Data directory: $PGDATA"
    echo "  Port:           $PGPORT"
    echo "  User:           $PGUSER"
    echo "  Database:       $PGDATABASE"
    echo ""

    init_database
    create_database

    echo ""
    echo "================================================================"
    echo "PostgreSQL initialization complete!"
    echo "================================================================"
    echo ""
    echo "Connection URL for Prefect:"
    echo "  postgresql+asyncpg://$PGUSER@localhost:$PGPORT/$PGDATABASE"
    echo ""
    echo "To start the Prefect server with PostgreSQL, run:"
    echo "  ./start_prefect_server.sh"
    echo ""
}

main "$@"
