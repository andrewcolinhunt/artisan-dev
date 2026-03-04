#!/bin/bash
#
# Stop Prefect server and PostgreSQL processes
#
# This script finds and kills all running Prefect server processes,
# and optionally stops the PostgreSQL database.
#
# Usage:
#   ./stop_prefect_server.sh           # Interactive (asks for confirmation)
#   ./stop_prefect_server.sh -f        # Force (no confirmation)
#   ./stop_prefect_server.sh --all     # Also stop PostgreSQL
#   ./stop_prefect_server.sh -f --all  # Force stop everything
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# UID-based port (same formula as start script)
PORT=$((4200 + ($(id -u) % 800)))

# Discovery file
DISCOVERY_FILE="$HOME/.artisan/prefect/server.json"

# PostgreSQL configuration
PGDATA="$PROJECT_ROOT/.prefect-postgres"

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

# Remove discovery file
remove_discovery_file() {
    if [ -f "$DISCOVERY_FILE" ]; then
        rm -f "$DISCOVERY_FILE"
        log_info "Discovery file removed: $DISCOVERY_FILE"
    fi
}

# Parse arguments
FORCE=false
STOP_POSTGRES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE=true
            shift
            ;;
        --all)
            STOP_POSTGRES=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-f|--force] [--all]"
            exit 1
            ;;
    esac
done

echo "================================================================"
echo "Prefect Server Stop Script"
echo "================================================================"
echo ""

# Find all Prefect server processes
echo "Searching for Prefect server processes..."
PIDS=$(ps aux | grep "prefect server" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
    echo "No Prefect server processes found."
    echo ""

    # Check if port is in use
    if command -v lsof &> /dev/null; then
        PORT_PID=$(lsof -ti:${PORT} 2>/dev/null || true)
        if [ -n "$PORT_PID" ]; then
            log_warn "Port ${PORT} is in use by process $PORT_PID"
            echo "This may not be a Prefect server. Process details:"
            ps -p $PORT_PID -o pid,user,cmd
            echo ""
            if [ "$FORCE" == true ]; then
                kill -9 $PORT_PID
                log_info "Process $PORT_PID killed"
            else
                read -p "Kill this process? (y/N) " -n 1 -r
                echo
                if [[ $REPLY =~ ^[Yy]$ ]]; then
                    kill -9 $PORT_PID
                    log_info "Process $PORT_PID killed"
                fi
            fi
        else
            echo "Port ${PORT} is free."
        fi
    fi
else
    echo "Found Prefect server process(es):"
    ps aux | grep "prefect server" | grep -v grep
    echo ""

    # Count processes
    NUM_PROCS=$(echo "$PIDS" | wc -w)
    echo "Total: $NUM_PROCS process(es)"
    echo ""

    # Ask for confirmation unless forced
    CONFIRMED=false
    if [ "$FORCE" == true ]; then
        CONFIRMED=true
        echo "Force mode: killing without confirmation"
    else
        read -p "Kill all Prefect server processes? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            CONFIRMED=true
        fi
    fi

    if [ "$CONFIRMED" == true ]; then
        echo ""
        echo "Stopping Prefect server processes..."

        # First try graceful shutdown (SIGTERM)
        for PID in $PIDS; do
            echo "  Sending SIGTERM to PID $PID..."
            kill $PID 2>/dev/null || true
        done

        # Wait a bit for graceful shutdown
        sleep 2

        # Check if any processes are still running
        REMAINING=$(ps aux | grep "prefect server" | grep -v grep | awk '{print $2}' || true)

        if [ -n "$REMAINING" ]; then
            echo ""
            echo "Some processes did not terminate gracefully. Force killing..."
            for PID in $REMAINING; do
                echo "  Sending SIGKILL to PID $PID..."
                kill -9 $PID 2>/dev/null || true
            done
            sleep 1
        fi

        # Final verification
        FINAL_CHECK=$(ps aux | grep "prefect server" | grep -v grep || true)

        if [ -z "$FINAL_CHECK" ]; then
            echo ""
            log_info "All Prefect server processes stopped successfully"
            remove_discovery_file
        else
            echo ""
            log_warn "Some processes may still be running:"
            echo "$FINAL_CHECK"
        fi
    else
        echo "Cancelled. No processes killed."
    fi
fi

# Stop PostgreSQL if requested
if [ "$STOP_POSTGRES" == true ]; then
    echo ""
    echo "----------------------------------------------------------------"
    echo "PostgreSQL"
    echo "----------------------------------------------------------------"

    if [ -f "$PGDATA/postmaster.pid" ]; then
        PG_PID=$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null || true)
        if [ -n "$PG_PID" ] && kill -0 "$PG_PID" 2>/dev/null; then
            log_info "Stopping PostgreSQL (PID: $PG_PID)..."
            "$PROJECT_ROOT/.pixi/envs/default/bin/pg_ctl" -D "$PGDATA" stop 2>/dev/null || {
                log_warn "Graceful stop failed, forcing..."
                kill -9 "$PG_PID" 2>/dev/null || true
            }
            log_info "PostgreSQL stopped"
        else
            log_info "PostgreSQL not running (stale PID file)"
            rm -f "$PGDATA/postmaster.pid"
        fi
    else
        log_info "PostgreSQL not running"
    fi
fi

echo ""
echo "================================================================"
echo "Done"
echo "================================================================"
