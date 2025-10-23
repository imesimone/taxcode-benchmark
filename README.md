# Italian Tax Code (Codice Fiscale) - KeyDB/PostgreSQL Benchmark

Performance benchmark for SHA256 hash computation and write operations on KeyDB/PostgreSQL using Italian tax codes (Codice Fiscale).

**Key Features**:
- **Fully containerized**: Python benchmark runs in Docker (no local setup required)
- Scalable to **65M+ records** with constant RAM usage (~32MB per mega-batch)
- Production-safe: **LOGGED tables** + **KeyDB persistence** (RDB + AOF)
- Parallel hash computation and database writes
- Independent benchmarks with separate data flows
- Configuration via `.env` file

---

## Quick Start

### Automated Execution (Recommended)

```bash
# Use the automated script (handles everything automatically)
./run_benchmark.sh
```

The script will:
- Detect system resources (CPU, RAM)
- Configure optimal parameters for KeyDB and PostgreSQL
- Create `.env` file from `.env.example` if not present
- Start KeyDB and PostgreSQL containers
- Build and run the benchmark container
- Display results

### Manual Execution

1. **Configure environment** (optional):
```bash
# Copy example configuration
cp .env.example .env

# Edit .env to customize parameters (optional)
nano .env
```

2. **Start all services**:
```bash
docker-compose up --build
```

This will:
- Start KeyDB with persistence enabled
- Start PostgreSQL with optimized settings
- Build and run the benchmark Python container

3. **Cleanup** (when done):
```bash
# Stop all containers
docker-compose down

# Stop and delete all data (including KeyDB/PostgreSQL volumes)
docker-compose down -v
```

---

## Benchmarks

The benchmark suite includes 5 independent tests:

### 1. Tax Code Generation
Generates 6M random Italian tax codes in parallel (no hash computation). Results stored in memory.

### 2. Write to PostgreSQL cf_raw
Writes raw tax codes to `cf_raw` table (immutable source, no hashes). Uses parallel COPY FROM STDIN for maximum throughput.

### 3. Hash Computation + KeyDB Write
Reads from `cf_raw` in mega-batches (2M at a time), computes SHA256 hashes, writes to KeyDB using pipelined operations.

**Independent loop**: Re-reads from `cf_raw` separately.

### 4. Hash Computation + PostgreSQL Write
Reads from `cf_raw` in mega-batches, computes SHA256 hashes, writes to `codici_fiscali` table using parallel COPY.

**Independent loop**: Re-reads from `cf_raw` separately.

### 5. Salt Rotation
Reads from `cf_raw` in mega-batches, recalculates all hashes with `NEW_SALT`, repopulates `codici_fiscali` using TRUNCATE + COPY strategy (10-20x faster than UPDATE).

**Independent execution**: Can run anytime after benchmark 2.

---

## Mega-Batch Architecture

The benchmark uses **mega-batch processing** to scale to 65M+ records with constant memory usage:

- **Mega-batch size**: 2M records per batch (configurable via `MEGA_BATCH_SIZE`)
- **Memory footprint**: ~32MB per mega-batch (constant, regardless of total dataset size)
- **Source independence**: Each benchmark independently re-reads from `cf_raw`
- **Server-side cursors**: PostgreSQL named cursors with `itersize` for efficient streaming

### Data Flow

```
BENCHMARK 1: Generate tax codes in memory
              ↓
BENCHMARK 2: Write to cf_raw (immutable source)
              ↓
         ┌────┴────┐
         ↓         ↓
    BENCHMARK 3   BENCHMARK 4
    (KeyDB)       (PostgreSQL)
    reads from    reads from
    cf_raw        cf_raw
    independently independently

         BENCHMARK 5
         (Salt Rotation)
         reads from cf_raw
         independently
```

Each benchmark (3, 4, 5) processes data in mega-batches:
1. Read 2M tax codes from `cf_raw`
2. Process (hash computation)
3. Write to target database
4. Repeat until all data processed

---

## Database Structure

### PostgreSQL

#### Table: `cf_raw`
```sql
CREATE TABLE cf_raw (
    codice_fiscale VARCHAR(16) PRIMARY KEY
);
```
**Purpose**: Immutable source of raw tax codes (no hashes). Used by benchmarks 3, 4, and 5.

**Note**: Always LOGGED (production-safe).

#### Table: `codici_fiscali`
```sql
CREATE TABLE codici_fiscali (
    hash VARCHAR(64) PRIMARY KEY,
    codice_fiscale VARCHAR(16) NOT NULL UNIQUE
);
```
**Structure**:
- `hash`: SHA256 hash (computed client-side)
- `codice_fiscale`: Italian tax code (16 chars)

**Note**: Always LOGGED (production-safe).

### KeyDB

**KeyDB** is a high-performance, multi-threaded, Redis-compatible in-memory database. Fully compatible with Redis protocol and commands.

**Key-Value structure**:
- **Key**: SHA256 hash (64 chars)
- **Value**: Tax code (16 chars)

**Why KeyDB**: Multi-threaded architecture provides better performance than Redis on multi-core systems, while maintaining 100% Redis compatibility.

---

## Configuration

### Configuration File (.env)

The benchmark uses a `.env` file for configuration. All parameters are optional with sensible defaults.

**Setup**:
```bash
# Copy the example configuration
cp .env.example .env

# Edit values as needed
nano .env
```

The `.env` file is automatically mounted into the benchmark container and loaded via `python-dotenv`.

**Note**: The automated script `run_benchmark.sh` creates `.env` from `.env.example` automatically if it doesn't exist.

### Environment Variables

All configuration parameters can be set in `.env` or passed as environment variables.

#### Connection Parameters

```bash
# KeyDB
KEYDB_HOST=localhost          # Default: localhost
KEYDB_PORT=6379               # Default: 6379
KEYDB_DB=0                    # Default: 0

# PostgreSQL
POSTGRES_HOST=localhost       # Default: localhost
POSTGRES_PORT=5432            # Default: 5432
POSTGRES_DB=cf_benchmark      # Default: cf_benchmark
POSTGRES_USER=postgres        # Default: postgres
POSTGRES_PASSWORD=postgres    # Default: postgres
```

#### Hashing Salts

```bash
CF_HASH_SALT="custom_salt_2025"    # Default: CF_ANPR_2025_SALT_KEY
NEW_SALT="new_salt_2026"           # For salt rotation (benchmark 5)
```

#### Benchmark Parameters

```bash
TOTAL_IDS=6000000                      # Total tax codes to generate
MEGA_BATCH_SIZE=2000000                # Records per mega-batch
BATCH_SIZE_COMPUTATION=100000          # Batch size for generation
BATCH_SIZE_CF_RAW=50000                # Batch size for cf_raw writes
BATCH_SIZE_KEYDB=10000                 # Batch size for KeyDB writes
BATCH_SIZE_POSTGRES=50000              # Batch size for PostgreSQL writes
BATCH_SIZE_POSTGRES_UPDATE=50000       # Batch size for salt rotation
```

#### Enable/Disable Benchmarks

```bash
RUN_POSTGRES_CF_WRITE=true       # Benchmark 2 (cf_raw write)
RUN_KEYDB=true                   # Benchmark 3 (KeyDB)
RUN_POSTGRES_INSERT=true         # Benchmark 4 (PostgreSQL)
RUN_POSTGRES_SALT_UPDATE=true    # Benchmark 5 (salt rotation)
```

### Example: Custom Configuration

Edit `.env` file for production-like configuration:

```bash
# .env file
TOTAL_IDS=10000000
POSTGRES_HOST=pg.example.com
POSTGRES_USER=cf_user
POSTGRES_PASSWORD=secure_pass
CF_HASH_SALT=production_salt_2025
```

Then run:
```bash
docker-compose up --build benchmark
```

---

## Performance Optimizations

### 1. Parallel Hash Computation
Uses Python `multiprocessing.Pool` to distribute hash computation across all CPU cores.

### 2. PostgreSQL COPY FROM STDIN
Bulk insert using `COPY` protocol (2-5x faster than INSERT statements).

### 3. Index Management
Drops indexes before bulk insert, recreates after. Dramatically faster for large datasets.

### 4. KeyDB Pipelining
Batches 10k SET operations in a single pipeline (eliminates network round-trips).

### 5. Connection Pooling
Reuses database connections across workers (reduces connection overhead).

### 6. Server-Side Cursors
PostgreSQL named cursors with `itersize` for memory-efficient data streaming.

### 7. Mega-Batch Processing
Processes data in 2M-record chunks to maintain constant memory usage regardless of dataset size.

---

## Salt Rotation Strategy (Benchmark 5)

### Problem
When rotating hashing salt, all hashes must be recalculated. Traditional UPDATE approach is slow:

```sql
-- ❌ SLOW: UPDATE on PRIMARY KEY (404s for 6M records)
UPDATE codici_fiscali
SET hash = new_hash
WHERE codice_fiscale = cf
```

**Why slow**: PostgreSQL must reorganize the B-tree index for every row (hash is PRIMARY KEY).

### Solution: TRUNCATE + COPY

```sql
-- ✅ FAST: TRUNCATE entire table + bulk insert (40s for 6M records)
TRUNCATE TABLE codici_fiscali;
COPY codici_fiscali (hash, codice_fiscale) FROM STDIN;
```

**Performance**: 10-20x faster than UPDATE approach.

**Strategy**:
1. Drop UNIQUE constraint on `codice_fiscale`
2. TRUNCATE table
3. Read tax codes from `cf_raw` in mega-batches
4. Recalculate hashes with `NEW_SALT`
5. Bulk insert using parallel COPY
6. Recreate UNIQUE constraint
7. Run ANALYZE

---

## Technical Details

### Tax Code Format

Italian Tax Code (Codice Fiscale): 16 alphanumeric characters

**Pattern**: `LLLLLLDDLDDLDDDL`
- `L` = Letter (A-Z)
- `D` = Digit (0-9)

**Example**: `RSSMRA85M01H501X`

### Hash Computation

```python
hash = SHA256(tax_code + salt).hexdigest()
```

- **Algorithm**: SHA256
- **Salt**: Configurable via `CF_HASH_SALT` environment variable
- **Output**: 64-character hexadecimal string

---

## Docker Compose Configuration

The benchmark uses Docker Compose to orchestrate three services:

1. **keydb**: High-performance in-memory database with persistence
2. **postgres**: Relational database for structured data storage
3. **benchmark**: Python application container that runs the benchmark

All services use `network_mode: host` for optimal performance (services accessible at localhost).

### Benchmark Container

The Python benchmark runs in a containerized environment:

```yaml
benchmark:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: cf_benchmark
  depends_on:
    - keydb
    - postgres
  network_mode: host
  volumes:
    - ./.env:/app/.env:ro  # Mount .env as read-only
  restart: "no"            # One-shot task
```

**Benefits of containerization**:
- ✅ Consistent Python environment across systems
- ✅ Isolated dependencies (no conflicts with system Python)
- ✅ Easy deployment and reproducibility
- ✅ No manual virtual environment setup required

**Image details**:
- Base: `python:3.11-slim`
- Dependencies: `redis`, `psycopg2-binary`, `python-dotenv`
- Size: ~200MB

### KeyDB Settings

```bash
export KEYDB_MEMORY=16gb    # Max memory
export KEYDB_THREADS=8      # Server threads
```

#### KeyDB Persistence (Production Mode)

KeyDB uses **dual persistence** mechanisms to ensure data safety in production:

**1. RDB (Redis Database) - Periodic Snapshots**

Creates binary snapshots of the entire dataset at configured intervals:

```yaml
--save "900 1"     # Save after 15 min if ≥1 key changed
--save "300 10"    # Save after 5 min if ≥10 keys changed
--save "60 10000"  # Save after 1 min if ≥10k keys changed
```

- **File**: `/data/dump.rdb` (stored in Docker volume `keydb_data`)
- **Pros**: Fast recovery, compact file size
- **Cons**: Potential data loss of last N seconds if crash occurs between snapshots

**2. AOF (Append-Only File) - Operation Log**

Logs every write operation to a persistent file:

```yaml
--appendonly yes        # Enable AOF logging
--appendfsync everysec  # Sync to disk every second
```

- **File**: `/data/appendonly.aof` (stored in Docker volume `keydb_data`)
- **Sync Strategy**: `everysec` (good balance between performance and safety)
  - `always`: Sync after every write (safest, slowest)
  - `everysec`: Sync every second (recommended, max 1s data loss)
  - `no`: Let OS decide when to sync (fastest, risky)
- **Pros**: Minimal data loss (max 1 second with `everysec`)
- **Cons**: Larger file size, slightly slower writes

**Why Persistence for Benchmarks?**

For **production benchmarks**, persistence must be enabled to:
- ✅ Simulate real production conditions (not theoretical max performance)
- ✅ Measure realistic write throughput with durability overhead
- ✅ Ensure benchmark results match production deployment
- ✅ Protect critical data (CF → hash mappings cannot be regenerated without original data)

**Performance Impact**: Persistence reduces write throughput by ~2-3x compared to in-memory-only mode, but this is the **realistic production performance**.

**Data Safety**: With dual persistence (RDB + AOF), data survives:
- Container restarts
- System crashes
- Power failures (max 1 second data loss with `everysec`)

### PostgreSQL Settings

PostgreSQL memory parameters have a significant impact on performance. Configure them based on your available RAM:

```bash
export PG_SHARED_BUFFERS=8GB          # Default: 8GB
export PG_WORK_MEM=256MB              # Default: 256MB
export PG_MAINTENANCE_WORK_MEM=2GB    # Default: 2GB
export PG_EFFECTIVE_CACHE_SIZE=16GB   # Default: 16GB
export PG_MAX_WAL_SIZE=4GB            # Default: 4GB
```

#### Memory Parameters Explained

**`shared_buffers`** (Default: 8GB)
- PostgreSQL's main memory buffer for caching data pages
- Used for reading and writing data blocks
- **Recommendation**: 25% of total RAM (e.g., 8GB for 32GB system)
- Higher values reduce disk I/O by keeping more data in memory
- Too high can cause performance degradation (leaves less RAM for OS cache)

**`work_mem`** (Default: 256MB)
- Memory allocated for **each** sort/hash operation in a query
- Multiple operations in a query can each use this amount
- **Recommendation**: Start with 256MB, monitor with complex queries
- Too low: Disk-based sorts (slow)
- Too high: Risk of OOM with many concurrent operations
- **Formula**: Available RAM / (max_connections × 2-3 operations per query)

**`maintenance_work_mem`** (Default: 2GB)
- Memory for maintenance operations: CREATE INDEX, ANALYZE, VACUUM
- Only one maintenance operation uses this at a time
- **Recommendation**: 1-2GB, or up to 2GB for large datasets
- Critical for index creation after bulk inserts (our use case)
- Higher values = faster index creation and ANALYZE

**`effective_cache_size`** (Default: 16GB)
- Estimate of memory available for disk caching (PostgreSQL + OS cache)
- Does NOT allocate memory, just helps query planner
- **Recommendation**: 50-75% of total RAM
- Example: 16GB for 32GB system, 32GB for 64GB system
- Influences whether PostgreSQL uses indexes or sequential scans

**`max_wal_size`** (Default: 4GB)
- Maximum size of Write-Ahead Log before checkpoint is triggered
- Larger values = less frequent checkpoints (better for bulk inserts)
- **Recommendation**: 2-4GB for bulk operations
- Trade-off: Larger WAL = longer recovery time after crash

#### Tuning for This Benchmark

For **bulk insert performance** (benchmarks 2, 3, 4):
- Increase `maintenance_work_mem` to speed up index creation
- Increase `max_wal_size` to reduce checkpoint frequency
- Increase `shared_buffers` to cache more data during writes

Example for a 32GB RAM system running bulk inserts:
```bash
export PG_SHARED_BUFFERS=8GB
export PG_WORK_MEM=256MB
export PG_MAINTENANCE_WORK_MEM=2GB
export PG_EFFECTIVE_CACHE_SIZE=20GB
export PG_MAX_WAL_SIZE=4GB

docker-compose up -d
```

Example for a 64GB RAM system:
```bash
export PG_SHARED_BUFFERS=16GB
export PG_WORK_MEM=512MB
export PG_MAINTENANCE_WORK_MEM=4GB
export PG_EFFECTIVE_CACHE_SIZE=40GB
export PG_MAX_WAL_SIZE=8GB

docker-compose up -d
```

---

## Important Notes

### Production Safety

✅ **Always uses LOGGED tables**
Tables defined in `init.sql` are production-safe

✅ **KeyDB persistence enabled**
Dual persistence (RDB + AOF) in production mode

✅ **Configurable via .env**
All parameters centralized in `.env` file

✅ **Independent benchmarks**
Can enable/disable via flags

✅ **Constant memory usage**
Mega-batch architecture scales to 65M+ records

✅ **Containerized execution**
Consistent environment across all systems

---

## Troubleshooting

### Out of Memory

If you encounter OOM errors:
1. Reduce `MEGA_BATCH_SIZE` (default: 2M)
2. Reduce `BATCH_SIZE_*` parameters
3. Increase available RAM

### Slow Performance

1. Check CPU usage (should be near 100% during hash computation)
2. Verify disk I/O (SSD recommended for large datasets)
3. Adjust PostgreSQL memory parameters
4. Increase `KEYDB_THREADS` for KeyDB

### Connection Errors

1. Verify Docker containers are running: `docker ps`
2. Check PostgreSQL logs: `docker logs cf_postgres_prod`
3. Check KeyDB logs: `docker logs cf_keydb_prod`
4. Check benchmark logs: `docker logs cf_benchmark`
5. Verify network connectivity: `nc -zv localhost 5432`

### Container Build Errors

1. Ensure `.env` file exists: `test -f .env && echo "OK" || cp .env.example .env`
2. Clean Docker cache: `docker-compose build --no-cache benchmark`
3. Check Docker disk space: `docker system df`
4. Prune unused images: `docker image prune -a`

### Configuration Issues

1. Verify `.env` file syntax: `cat .env | grep -v '^#' | grep -v '^$'`
2. Check for missing variables: `diff <(grep -o '^[A-Z_]*=' .env.example | sort) <(grep -o '^[A-Z_]*=' .env | sort)`
3. Reset to defaults: `cp .env.example .env`

---

## License

**CC0 1.0 Universal (Public Domain)**

This work has been dedicated to the public domain. You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission.

See [LICENSE](https://creativecommons.org/publicdomain/zero/1.0/) for details.

Do whatever you want with this code.

---

## Credits

Benchmark optimizations based on PostgreSQL and Redis best practices for bulk data operations.

**KeyDB** is a high-performance, Redis-compatible database with multi-threading support. Fully compatible with Redis protocol and commands.
