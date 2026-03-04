#!/bin/bash
#
# Start Prefect server with PostgreSQL backend
#
# This starts the Prefect server configured for:
#   - PostgreSQL backend (handles SLURM concurrency better than SQLite)
#   - UID-based port (avoids conflicts on shared nodes)
#   - FQDN hostname (works cross-node for SLURM workers)
#   - Discovery file for automatic Python-side connection
#   - Remote access via SSH tunneling
#
# The server binds to all interfaces (0.0.0.0) and the UI is configured
# to use localhost, which works correctly with SSH port forwarding.
#
# Usage:
#   ./start_prefect_server.sh           # Start in foreground (Ctrl+C to stop)
#   ./start_prefect_server.sh --bg      # Start in background
#   ./start_prefect_server.sh --restart # Stop existing server and start fresh
#   ./start_prefect_server.sh --sqlite  # Use SQLite instead of PostgreSQL
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$SCRIPT_DIR"

# UID-based port to avoid conflicts on shared nodes (range 4200-4999)
PORT=$((4200 + ($(id -u) % 800)))

# FQDN for cross-node access (SLURM workers on compute nodes)
SERVER_HOST=$(hostname -f)
SERVER_URL="http://${SERVER_HOST}:${PORT}/api"

# Discovery file for automatic Python-side connection
DISCOVERY_DIR="$HOME/.artisan/prefect"
DISCOVERY_FILE="$DISCOVERY_DIR/server.json"

# Prefect binary (resolved before if/else branch)
PREFECT_BIN="$PROJECT_ROOT/.pixi/envs/default/bin/prefect"

# PostgreSQL configuration
PGDATA="$PROJECT_ROOT/.prefect-postgres"
PGPORT=5433
PGUSER="prefect"
PGDATABASE="prefect"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check for existing Prefect server processes
find_prefect_servers() {
    pgrep -f "prefect server start" 2>/dev/null || true
}

# Check if a Prefect server is already running (process or port)
check_existing_server() {
    local existing_pids
    existing_pids=$(find_prefect_servers)

    if [ -n "$existing_pids" ]; then
        log_error "Prefect server is already running"
        echo ""
        echo "Existing process(es):"
        ps -p $existing_pids -o pid,user,cmd 2>/dev/null || true
        echo ""
        echo "To stop existing servers, run:"
        echo "  ./stop_prefect_server.sh"
        echo ""
        echo "Or use --restart to automatically stop and restart:"
        echo "  $0 --restart [--bg]"
        exit 1
    fi

    # Also check port as a fallback (in case process detection missed something)
    if command -v lsof &> /dev/null; then
        local port_pid
        port_pid=$(lsof -ti:${PORT} 2>/dev/null || true)
        if [ -n "$port_pid" ]; then
            log_error "Port ${PORT} is already in use by process $port_pid"
            echo ""
            ps -p $port_pid -o pid,user,cmd 2>/dev/null || true
            echo ""
            echo "To stop existing servers, run:"
            echo "  ./stop_prefect_server.sh"
            echo ""
            echo "Or use --restart to automatically stop and restart:"
            echo "  $0 --restart [--bg]"
            exit 1
        fi
    fi
}

# Stop any existing Prefect server processes
stop_existing_servers() {
    local existing_pids
    existing_pids=$(find_prefect_servers)

    if [ -n "$existing_pids" ]; then
        log_info "Stopping existing Prefect server(s)..."
        for pid in $existing_pids; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
                # Wait for process to stop (max 5 seconds)
                local count=0
                while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
                    sleep 0.5
                    count=$((count + 1))
                done
                if kill -0 "$pid" 2>/dev/null; then
                    log_warn "Process $pid didn't stop gracefully, sending SIGKILL..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
            fi
        done
        log_info "Existing server(s) stopped"
        # Give the port a moment to be released
        sleep 1
    fi
}

# Stop PostgreSQL if running (used during --restart for a clean slate)
stop_postgres() {
    if [ -f "$PGDATA/postmaster.pid" ]; then
        local pg_pid
        pg_pid=$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null || true)
        if [ -n "$pg_pid" ] && kill -0 "$pg_pid" 2>/dev/null; then
            log_info "Stopping PostgreSQL (PID: $pg_pid)..."
            "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" -D "$PGDATA" stop -m fast 2>/dev/null || {
                log_warn "Graceful stop failed, sending SIGKILL..."
                kill -9 "$pg_pid" 2>/dev/null || true
            }
            log_info "PostgreSQL stopped"
        else
            # Stale PID file
            rm -f "$PGDATA/postmaster.pid"
        fi
    fi
}

# Write discovery file for Python-side auto-detection
write_discovery_file() {
    mkdir -p "$DISCOVERY_DIR"
    cat > "$DISCOVERY_FILE" << EOF
{
    "url": "$SERVER_URL",
    "host": "$SERVER_HOST",
    "port": $PORT,
    "pid": $SERVER_PID,
    "user": "$(whoami)",
    "project_root": "$PROJECT_ROOT",
    "backend": "$([ "$USE_SQLITE" = true ] && echo sqlite || echo postgres)",
    "started": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    log_info "Discovery file written: $DISCOVERY_FILE"
}

# Remove discovery file on shutdown
remove_discovery_file() {
    if [ -f "$DISCOVERY_FILE" ]; then
        rm -f "$DISCOVERY_FILE"
        log_info "Discovery file removed: $DISCOVERY_FILE"
    fi
}

# Initialize PostgreSQL if not already done
init_postgres_if_needed() {
    if [ ! -d "$PGDATA" ] || [ ! -f "$PGDATA/PG_VERSION" ]; then
        log_info "PostgreSQL not initialized, running init_postgres.sh..."
        "$SCRIPT_DIR/init_postgres.sh"
    fi
}

# Start PostgreSQL if not running
start_postgres() {
    # Initialize if needed
    init_postgres_if_needed

    # Always sync config (idempotent)
    "$SCRIPT_DIR/init_postgres.sh" --update-config

    # Check if PostgreSQL is already running
    if [ -f "$PGDATA/postmaster.pid" ]; then
        # Verify it's actually running
        PG_PID=$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null || true)
        if [ -n "$PG_PID" ] && kill -0 "$PG_PID" 2>/dev/null; then
            log_info "PostgreSQL already running (PID: $PG_PID)"
            return 0
        else
            # Stale PID file, remove it
            rm -f "$PGDATA/postmaster.pid"
        fi
    fi

    log_info "Starting PostgreSQL..."
    "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" \
        -D "$PGDATA" \
        -l "$PGDATA/postgres.log" \
        -o "-p $PGPORT" \
        start

    # Wait for PostgreSQL to be ready
    local count=0
    while ! "$PROJECT_ROOT/.pixi/envs/default/bin/pg_isready" -h localhost -p "$PGPORT" -q 2>/dev/null; do
        sleep 0.5
        count=$((count + 1))
        if [ $count -gt 20 ]; then
            log_error "PostgreSQL failed to start"
            echo "Check logs at: $PGDATA/postgres.log"
            exit 1
        fi
    done

    log_info "PostgreSQL started on port $PGPORT"
}

# Parse arguments
BACKGROUND=false
USE_SQLITE=false
RESTART=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --bg)
            BACKGROUND=true
            shift
            ;;
        --sqlite)
            USE_SQLITE=true
            shift
            ;;
        --restart)
            RESTART=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--bg] [--sqlite] [--restart]"
            exit 1
            ;;
    esac
done

# Check prefect binary exists
if [ ! -f "$PREFECT_BIN" ]; then
    log_error "Could not find prefect binary at $PREFECT_BIN"
    echo "Make sure pixi environment is set up."
    exit 1
fi

# Handle restart or check for existing server
if [ "$RESTART" = true ]; then
    stop_existing_servers
    stop_postgres
else
    check_existing_server
fi

echo "================================================================"
echo "Starting Prefect Server"
echo "================================================================"
echo ""

# Configure database backend
if [ "$USE_SQLITE" = true ]; then
    log_warn "Using SQLite backend (not recommended for SLURM workloads)"
    echo "  Database: SQLite (~/.prefect/prefect.db)"
    # Don't set PREFECT_API_DATABASE_CONNECTION_URL, use default SQLite
else
    start_postgres
    export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://$PGUSER@localhost:$PGPORT/$PGDATABASE"
    log_info "Using PostgreSQL backend"
    echo "  Database: PostgreSQL (localhost:$PGPORT/$PGDATABASE)"
fi

echo ""
echo "Server will be accessible at:"
echo "  Local:      http://localhost:${PORT}"
echo "  SSH tunnel: ssh -L ${PORT}:localhost:${PORT} ${SERVER_HOST}"
echo "              then open http://localhost:${PORT}"
echo "  Network:    http://${SERVER_HOST}:${PORT}"
echo ""
echo "For remote clients (e.g., SLURM workers), set:"
echo "  export PREFECT_API_URL=\"${SERVER_URL}\""
echo ""

# Configure Prefect server to listen on all interfaces and set UI API URL
export PREFECT_SERVER_API_HOST="0.0.0.0"
export PREFECT_UI_API_URL="http://localhost:${PORT}/api"

# Database connection pool tuning (defaults: pool_size=5, max_overflow=10 = 15 total)
# With 1000+ concurrent SLURM tasks reporting state, 15 connections is easily exhausted.
# pool_size=50 + max_overflow=120 = 170 total, within PG max_connections=200.
export PREFECT_SERVER_DATABASE_SQLALCHEMY_POOL_SIZE=50
export PREFECT_SERVER_DATABASE_SQLALCHEMY_MAX_OVERFLOW=120

# Ensure Prefect profile points to FQDN (works cross-node for SLURM and JupyterHub)
"$PREFECT_BIN" config set "PREFECT_API_URL=${SERVER_URL}" > /dev/null 2>&1 || true

if [ "$BACKGROUND" == true ]; then
    # Start in background
    LOG_DIR="$SCRIPT_DIR/prefect-logs"
    mkdir -p "$LOG_DIR"
    LOG_FILE="$LOG_DIR/prefect-server-$(date +%Y%m%d-%H%M%S).log"
    echo "Starting server in background..."
    echo "  Log file: $LOG_FILE"
    echo ""

    nohup "$PREFECT_BIN" server start --host 0.0.0.0 --port $PORT > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!

    write_discovery_file

    echo "Server started with PID: $SERVER_PID"
    echo ""
    echo "To check if it's running:"
    echo "  ps -p $SERVER_PID"
    echo ""
    echo "To view logs:"
    echo "  tail -f $LOG_FILE"
    echo ""
    echo "To stop the server:"
    echo "  ./stop_prefect_server.sh"
    echo "  or: kill $SERVER_PID"
else
    # Start in foreground using background+wait pattern to get PID for discovery file
    echo "Press Ctrl+C to stop the server"
    echo ""
    echo "================================================================"
    echo ""

    "$PREFECT_BIN" server start --host 0.0.0.0 --port $PORT &
    SERVER_PID=$!

    write_discovery_file

    trap 'remove_discovery_file; kill $SERVER_PID 2>/dev/null' EXIT INT TERM
    wait $SERVER_PID
fi
