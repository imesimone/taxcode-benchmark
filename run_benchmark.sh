#!/usr/bin/env bash
set -e  # Esci immediatamente in caso di errore

# ============================================================================
# Script di avvio benchmark CF -> SHA256 con MASSIME PERFORMANCE
# Configura automaticamente Docker e Python per usare TUTTE le risorse
# ============================================================================

echo "=========================================="
echo "🚀 BENCHMARK CF -> SHA256 HASH"
echo "=========================================="
echo ""

# Rileva sistema operativo
OS=$(uname -s)

# ============================================================================
# FASE 1: RILEVAMENTO RISORSE
# ============================================================================
echo "📊 Rilevamento risorse disponibili..."
echo ""

# Rileva numero di CPU
if [[ "$OS" == "Darwin" ]]; then
    # macOS
    CPU_COUNT=$(sysctl -n hw.ncpu)
    TOTAL_RAM_GB=$(sysctl -n hw.memsize | awk '{print int($1/1024/1024/1024)}')
elif [[ "$OS" == "Linux" ]]; then
    # Linux
    CPU_COUNT=$(nproc)
    TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
else
    echo "⚠️  Sistema operativo non riconosciuto, uso valori predefiniti"
    CPU_COUNT=4
    TOTAL_RAM_GB=16
fi

echo "   ✓ CPU disponibili: $CPU_COUNT"
echo "   ✓ RAM totale: ${TOTAL_RAM_GB}GB"
echo ""

# ============================================================================
# FASE 2: CALCOLO PARAMETRI OTTIMALI
# ============================================================================
echo "⚙️  Calcolo parametri ottimali per massime performance..."
echo ""

# KeyDB: usa tutte le CPU, 40% della RAM
export KEYDB_THREADS=$CPU_COUNT
KEYDB_MEMORY_GB=$(echo "$TOTAL_RAM_GB * 0.4" | bc | awk '{print int($1)}')
export KEYDB_MEMORY="${KEYDB_MEMORY_GB}gb"

# PostgreSQL: configurazione ottimale secondo best practice
# shared_buffers: 25% della RAM (max 40% se solo PostgreSQL)
PG_SHARED_BUFFERS_GB=$(echo "$TOTAL_RAM_GB * 0.25" | bc | awk '{print int($1)}')
export PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS_GB}GB"

# work_mem: RAM / (max_connections * 2), min 256MB
PG_WORK_MEM_MB=$(echo "$TOTAL_RAM_GB * 1024 / 400" | bc | awk '{print int($1)}')
if [ $PG_WORK_MEM_MB -lt 256 ]; then
    PG_WORK_MEM_MB=256
fi
export PG_WORK_MEM="${PG_WORK_MEM_MB}MB"

# maintenance_work_mem: 5% della RAM o 2GB, quello maggiore
PG_MAINTENANCE_GB=$(echo "$TOTAL_RAM_GB * 0.05" | bc | awk '{print int($1)}')
if [ $PG_MAINTENANCE_GB -lt 2 ]; then
    PG_MAINTENANCE_GB=2
fi
export PG_MAINTENANCE_WORK_MEM="${PG_MAINTENANCE_GB}GB"

# effective_cache_size: 50-75% della RAM
PG_CACHE_GB=$(echo "$TOTAL_RAM_GB * 0.6" | bc | awk '{print int($1)}')
export PG_EFFECTIVE_CACHE_SIZE="${PG_CACHE_GB}GB"

# max_wal_size: 4GB standard (può essere aumentato se RAM > 64GB)
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
# FASE 3: PULIZIA E AVVIO SERVIZI
# ============================================================================
echo "🧹 Pulizia container precedenti..."
docker-compose down -v 2>/dev/null || true
echo ""

echo "🐳 Avvio servizi Docker con massime risorse..."
docker-compose -f docker-compose.production.yml up -d

echo ""
echo "⏳ Attesa avvio servizi..."
sleep 5

# ============================================================================
# FASE 4: HEALTH CHECK
# ============================================================================
echo ""
echo "🔍 Verifica disponibilità servizi..."

# Detect container names (supports both benchmark and production modes)
KEYDB_CONTAINER=$(docker ps --filter "name=cf_keydb" --format "{{.Names}}" | head -1)
POSTGRES_CONTAINER=$(docker ps --filter "name=cf_postgres" --format "{{.Names}}" | head -1)

if [ -z "$KEYDB_CONTAINER" ]; then
    echo "❌ Errore: Nessun container KeyDB trovato"
    exit 1
fi

if [ -z "$POSTGRES_CONTAINER" ]; then
    echo "❌ Errore: Nessun container PostgreSQL trovato"
    exit 1
fi

# Attendi KeyDB
echo -n "   KeyDB ($KEYDB_CONTAINER): "
MAX_RETRIES=30
for i in $(seq 1 $MAX_RETRIES); do
    if docker exec $KEYDB_CONTAINER keydb-cli ping 2>/dev/null | grep -q PONG; then
        echo "✓ Ready"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "✗ TIMEOUT"
        echo "❌ Errore: KeyDB non risponde"
        exit 1
    fi
    sleep 1
done

# Attendi PostgreSQL
echo -n "   PostgreSQL ($POSTGRES_CONTAINER): "
for i in $(seq 1 $MAX_RETRIES); do
    if docker exec $POSTGRES_CONTAINER pg_isready -U postgres 2>/dev/null | grep -q "accepting connections"; then
        echo "✓ Ready"
        break
    fi
    if [ $i -eq $MAX_RETRIES ]; then
        echo "✗ TIMEOUT"
        echo "❌ Errore: PostgreSQL non risponde"
        exit 1
    fi
    sleep 1
done

echo ""

# ============================================================================
# FASE 4.5: VERIFICA SCHEMA DATABASE
# ============================================================================
echo "🔍 Verifica schema database PostgreSQL..."

# Verifica se la tabella codici_fiscali esiste
TABLE_EXISTS=$(docker exec $POSTGRES_CONTAINER psql -U postgres -d cf_benchmark -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'codici_fiscali');" 2>/dev/null || echo "false")

if [ "$TABLE_EXISTS" = "t" ] || [ "$TABLE_EXISTS" = "true" ]; then
    echo "   ✓ Tabella codici_fiscali trovata"
else
    echo "   ✗ Tabella codici_fiscali NON trovata"
    echo ""
    echo "   Esecuzione init.sql..."

    # Esegui init.sql nel container
    if docker exec -i $POSTGRES_CONTAINER psql -U postgres -d cf_benchmark < init.sql 2>/dev/null; then
        echo "   ✓ Schema database creato con successo"
    else
        echo "   ✗ Errore durante la creazione dello schema"
        echo ""
        echo "❌ ERRORE: Impossibile inizializzare il database"
        echo "   Soluzione: controlla che init.sql sia corretto"
        exit 1
    fi
fi

echo ""

# ============================================================================
# FASE 5: VERIFICA AMBIENTE PYTHON
# ============================================================================
echo "🐍 Verifica ambiente Python..."

# Controlla se esiste venv
if [ -d ".venv" ]; then
    echo "   ✓ Virtual environment trovato"
    source .venv/bin/activate
else
    echo "   ⚠️  Virtual environment non trovato"
    echo "   Creazione virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate

    echo "   Installazione dipendenze..."
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
fi

echo ""

# ============================================================================
# FASE 6: ESECUZIONE BENCHMARK
# ============================================================================
echo "=========================================="
echo "🔥 AVVIO BENCHMARK - MASSIME PERFORMANCE"
echo "=========================================="
echo ""
echo "Configurazione:"
echo "   - CPU: $CPU_COUNT core"
echo "   - RAM: ${TOTAL_RAM_GB}GB"
echo "   - KeyDB: $KEYDB_MEMORY (${KEYDB_THREADS} threads)"
echo "   - PostgreSQL: ${PG_SHARED_BUFFERS} shared_buffers"
echo ""
echo "Press CTRL+C to stop..."
echo ""

# Esegui il benchmark
python3 main.py

# ============================================================================
# FASE 7: CLEANUP (opzionale)
# ============================================================================
echo ""
echo "=========================================="
echo "✅ BENCHMARK COMPLETATO"
echo "=========================================="
echo ""

read -p "Vuoi fermare i servizi Docker? [y/N] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧹 Pulizia servizi..."
    docker-compose down
    echo "   ✓ Servizi fermati"
else
    echo "   ℹ️  Servizi ancora in esecuzione. Per fermarli: docker-compose down"
fi

echo ""
echo "🎉 Done!"
