#!/usr/bin/env bash
set -e  # Exit immediately on error

# ============================================================================
# Benchmark startup script with automatic Docker and Python configuration
# Automatically configures resources for maximum performance
# ============================================================================

echo "=========================================="
echo "🚀 BENCHMARK: TAX CODE -> SHA256 HASH"
echo "=========================================="
echo ""

# Detect operating system
OS=$(uname -s)

# ============================================================================
# PHASE 1: RESOURCE DETECTION
# ============================================================================
echo "📊 Detecting available resources..."
echo ""

# Detect CPU count and RAM based on OS
if [[ "$OS" == "Darwin" ]]; then
    # macOS
    CPU_COUNT=$(sysctl -n hw.ncpu)
    TOTAL_RAM_GB=$(sysctl -n hw.memsize | awk '{print int($1/1024/1024/1024)}')
elif [[ "$OS" == "Linux" ]]; then
    # Linux
    CPU_COUNT=$(nproc)
    TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
elif [[ "$OS" =~ ^(MINGW|MSYS|CYGWIN) ]]; then
    # Windows (Git Bash, MSYS2, Cygwin)
    echo "   Detected Windows environment"
    # CPU count from NUMBER_OF_PROCESSORS environment variable
    CPU_COUNT=${NUMBER_OF_PROCESSORS:-4}
    # RAM detection using wmic (if available)
    if command -v wmic.exe &> /dev/null; then
        TOTAL_RAM_BYTES=$(wmic.exe computersystem get TotalPhysicalMemory | sed -n '2p' | tr -d '\r\n ')
        TOTAL_RAM_GB=$((TOTAL_RAM_BYTES / 1024 / 1024 / 1024))
    else
        echo "   ⚠️  wmic not available, using default RAM value"
        TOTAL_RAM_GB=16
    fi
else
    echo "⚠️  Unrecognized operating system: $OS"
    echo "   Using default values"
    CPU_COUNT=4
    TOTAL_RAM_GB=16
fi

echo "   ✓ Available CPUs: $CPU_COUNT"
echo "   ✓ Total RAM: ${TOTAL_RAM_GB}GB"
echo ""

# ============================================================================
# PHASE 2: CALCULATE OPTIMAL PARAMETERS
# ============================================================================
echo "⚙️  Calculating optimal parameters for maximum performance..."
echo ""

# KeyDB: use all CPUs, 40% of RAM
export KEYDB_THREADS=$CPU_COUNT
KEYDB_MEMORY_GB=$(( TOTAL_RAM_GB * 40 / 100 ))
[ $KEYDB_MEMORY_GB -lt 1 ] && KEYDB_MEMORY_GB=1
export KEYDB_MEMORY="${KEYDB_MEMORY_GB}gb"

# PostgreSQL: optimal configuration following best practices
# shared_buffers: 25% of RAM (max 40% if PostgreSQL only)
PG_SHARED_BUFFERS_GB=$(( TOTAL_RAM_GB * 25 / 100 ))
[ $PG_SHARED_BUFFERS_GB -lt 1 ] && PG_SHARED_BUFFERS_GB=1
export PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS_GB}GB"

# work_mem: RAM / (max_connections * 2), min 256MB
PG_WORK_MEM_MB=$(( TOTAL_RAM_GB * 1024 / 400 ))
if [ $PG_WORK_MEM_MB -lt 256 ]; then
    PG_WORK_MEM_MB=256
fi
export PG_WORK_MEM="${PG_WORK_MEM_MB}MB"

# maintenance_work_mem: 5% of RAM or 2GB, whichever is greater
PG_MAINTENANCE_GB=$(( TOTAL_RAM_GB * 5 / 100 ))
if [ $PG_MAINTENANCE_GB -lt 2 ]; then
    PG_MAINTENANCE_GB=2
fi
export PG_MAINTENANCE_WORK_MEM="${PG_MAINTENANCE_GB}GB"

# effective_cache_size: 50-75% of RAM
PG_CACHE_GB=$(( TOTAL_RAM_GB * 60 / 100 ))
[ $PG_CACHE_GB -lt 1 ] && PG_CACHE_GB=1
export PG_EFFECTIVE_CACHE_SIZE="${PG_CACHE_GB}GB"

# max_wal_size: 4GB standard (can be increased if RAM > 64GB)
if [ $TOTAL_RAM_GB -gt 64 ]; then
    export PG_MAX_WAL_SIZE="8GB"
else
    export PG_MAX_WAL_SIZE="4GB"
fi

echo "   KeyDB:"
echo "      - Threads: $KEYDB_THREADS"
echo "      - Memory: $KEYDB_MEMORY"
echo ""
echo "   PostgreSQL:"
echo "      - shared_buffers: $PG_SHARED_BUFFERS"
echo "      - work_mem: $PG_WORK_MEM"
echo "      - maintenance_work_mem: $PG_MAINTENANCE_WORK_MEM"
echo "      - effective_cache_size: $PG_EFFECTIVE_CACHE_SIZE"
echo "      - max_wal_size: $PG_MAX_WAL_SIZE"
echo ""

# ============================================================================
# PHASE 3: CLEANUP AND START SERVICES
# ============================================================================

# Detect docker-compose command (supports both "docker-compose" and "docker compose")
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
else
    echo "❌ Error: Neither 'docker-compose' nor 'docker compose' found"
    echo "   Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "   Using: $DOCKER_COMPOSE"
echo ""

echo "🧹 Cleaning up previous containers..."
$DOCKER_COMPOSE down -v 2>/dev/null || true
echo ""

echo "🐳 Starting Docker services with maximum resources..."
$DOCKER_COMPOSE up -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 5

# ============================================================================
# PHASE 4: HEALTH CHECK
# ============================================================================
echo ""
echo "🔍 Checking service availability..."

# Detect container names (supports both benchmark and production modes)
KEYDB_CONTAINER=$(docker ps --filter "name=cf_keydb" --format "{{.Names}}" | head -1)
POSTGRES_CONTAINER=$(docker ps --filter "name=cf_postgres" --format "{{.Names}}" | head -1)

if [ -z "$KEYDB_CONTAINER" ]; then
    echo "❌ Error: No KeyDB container found"
    exit 1
fi

if [ -z "$POSTGRES_CONTAINER" ]; then
    echo "❌ Error: No PostgreSQL container found"
    exit 1
fi

# Wait for KeyDB
echo -n "   KeyDB ($KEYDB_CONTAINER): "
MAX_RETRIES=120  # 2 minutes timeout
for i in $(seq 1 $MAX_RETRIES); do
    if docker exec $KEYDB_CONTAINER keydb-cli ping 2>/dev/null | grep -q PONG; then
        echo "✓ Ready"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "✗ TIMEOUT"
        echo "❌ Error: KeyDB not responding after 2 minutes"
        exit 1
    fi
    sleep 1
done

# Wait for PostgreSQL
echo -n "   PostgreSQL ($POSTGRES_CONTAINER): "
for i in $(seq 1 $MAX_RETRIES); do
    if docker exec $POSTGRES_CONTAINER pg_isready -U postgres 2>/dev/null | grep -q "accepting connections"; then
        echo "✓ Ready"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "✗ TIMEOUT"
        echo "❌ Error: PostgreSQL not responding after 2 minutes"
        exit 1
    fi
    sleep 1
done

echo ""

# ============================================================================
# PHASE 4.5: VERIFY DATABASE SCHEMA
# ============================================================================
echo "🔍 Verifying PostgreSQL database schema..."

# Check if codici_fiscali table exists
TABLE_EXISTS=$(docker exec $POSTGRES_CONTAINER psql -U postgres -d cf_benchmark -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'codici_fiscali');" 2>/dev/null || echo "false")

if [ "$TABLE_EXISTS" = "t" ] || [ "$TABLE_EXISTS" = "true" ]; then
    echo "   ✓ Table codici_fiscali found"
else
    echo "   ✗ Table codici_fiscali NOT found"
    echo ""
    echo "   Running init.sql..."

    # Execute init.sql in container
    if docker exec -i $POSTGRES_CONTAINER psql -U postgres -d cf_benchmark < init.sql 2>/dev/null; then
        echo "   ✓ Database schema created successfully"
    else
        echo "   ✗ Error during schema creation"
        echo ""
        echo "❌ ERROR: Unable to initialize database"
        echo "   Solution: check that init.sql is correct"
        exit 1
    fi
fi

echo ""

# ============================================================================
# PHASE 5: VERIFY .ENV FILE
# ============================================================================
echo "📝 Verifying environment configuration..."

if [ ! -f ".env" ]; then
    echo "   ⚠️  .env file not found"
    echo "   Creating .env from .env.example..."
    cp .env.example .env
    echo "   ✓ .env file created"
    echo "   ℹ️  Please review .env and adjust values if needed"
else
    echo "   ✓ .env file found"
fi

echo ""

# ============================================================================
# PHASE 6: BUILD AND RUN BENCHMARK CONTAINER
# ============================================================================
echo "=========================================="
echo "🔥 STARTING BENCHMARK - MAXIMUM PERFORMANCE"
echo "=========================================="
echo ""
echo "Configuration:"
echo "   - CPU: $CPU_COUNT cores"
echo "   - RAM: ${TOTAL_RAM_GB}GB"
echo "   - KeyDB: $KEYDB_MEMORY (${KEYDB_THREADS} threads)"
echo "   - PostgreSQL: ${PG_SHARED_BUFFERS} shared_buffers"
echo ""
echo "📦 Building and running benchmark container..."
echo ""

# Build and run the benchmark container
$DOCKER_COMPOSE up --build benchmark

# ============================================================================
# PHASE 7: CLEANUP (optional)
# ============================================================================
echo ""
echo "=========================================="
echo "✅ BENCHMARK COMPLETED"
echo "=========================================="
echo ""

read -p "Stop Docker services? [y/N] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧹 Cleaning up services..."
    $DOCKER_COMPOSE down
    echo "   ✓ Services stopped"
else
    echo "   ℹ️  Services still running. To stop: $DOCKER_COMPOSE down"
fi

echo ""
echo "🎉 Done!"
