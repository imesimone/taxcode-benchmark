# Italian Tax Code (Codice Fiscale) - KeyDB/PostgreSQL Benchmark

Performance benchmark comparing SHA256 hash computation and write operations on KeyDB/PostgreSQL for 6 million Italian tax codes.

## Quick Start

### Benchmark Mode (Maximum Performance)

Optimized for benchmarking with UNLOGGED TABLE and fsync=off:

```bash
# Launch with benchmark configuration (default)
docker-compose up -d

# Or use the automated script
./run_benchmark.sh

# Execute the benchmark
python main.py
```

### Production Mode (Data Safety)

Secure configuration with LOGGED TABLE and fsync=on:

```bash
# Launch with production configuration
docker-compose -f docker-compose.production.yml up -d

# Execute with LOGGED TABLE
USE_UNLOGGED_TABLE=false python main.py
```

### Configuration Comparison

| Parameter | Benchmark | Production |
|-----------|-----------|------------|
| PostgreSQL fsync | `off` | `on` |
| PostgreSQL synchronous_commit | `off` | `on` |
| codici_fiscali table | UNLOGGED | LOGGED |
| KeyDB persistence | Disabled | AOF + Snapshot |
| Insert performance | ~3-5x faster | Normal |
| Data safety | ⚠️ Risk of loss | ✅ Safe |

### Manual Execution

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start Docker containers (choose configuration):
```bash
# Benchmark (maximum performance)
docker-compose up -d

# Production (data safety)
docker-compose -f docker-compose.production.yml up -d
```

3. Execute the benchmark:
```bash
# With UNLOGGED TABLE (default, faster)
python main.py

# With LOGGED TABLE (safe)
USE_UNLOGGED_TABLE=false python main.py
```

## Included Benchmarks

**New Architecture** - Tax codes are generated once, stored in PostgreSQL, then read for hash computation:

1. **Tax Code Generation** - Generate 6M tax codes in parallel (no hash computation). Stored in memory.
2. **BENCHMARK 1bis: Write to PostgreSQL cf_raw** - Write tax codes to PostgreSQL `cf_raw` table (no hashes, just raw data)
3. **Read from PostgreSQL** - Read tax codes from `cf_raw` table for subsequent benchmarks
4. **BENCHMARK 2: Hash Computation + KeyDB Write** - Read tax codes, compute SHA256 hashes, write to KeyDB
5. **BENCHMARK 3: Hash Computation + PostgreSQL Write** - Read tax codes, compute SHA256 hashes, write to PostgreSQL `codici_fiscali` table
6. **BENCHMARK 4: Salt Rotation (optional)** - Recalculate and replace all hashes with new salt using TRUNCATE + COPY strategy (10-20x faster than UPDATE)

---

## Performance Optimization Techniques

### 1. Tax Code Generation

#### Problem
Generating 6 million tax codes is a CPU-intensive operation that can become a bottleneck.

#### Implemented Solutions

**A. Single Generation and Reuse**
```python
# ❌ SLOW: Generate tax codes multiple times
benchmark_keydb(6_000_000)  # generates 6M tax codes
benchmark_postgres(6_000_000)  # regenerates 6M tax codes

# ✅ FAST: Generate once, reuse
all_tax_codes = generate_all_tax_codes(6_000_000)  # generate once
benchmark_keydb(6_000_000, tax_code_list=all_tax_codes)  # reuse
benchmark_postgres(6_000_000, tax_code_list=all_tax_codes)  # reuse
```
**Gain**: Eliminates unnecessary regeneration, 50% total time savings

**B. Pre-computed Patterns and Tuples**
```python
# ❌ SLOW: Convert on each iteration
random.choice(string.ascii_uppercase)  # expensive conversion

# ✅ FAST: Pre-compute tuples
LETTERS_TUPLE = tuple(string.ascii_uppercase)
random.choice(LETTERS_TUPLE)  # direct access
```
**Gain**: ~15% faster tax code generation

**C. Multiprocessing with Pool.map()**
```python
# Use all available CPU cores
with Pool(processes=cpu_count()) as pool:
    results = pool.map(generate_tax_code_batch_worker, batch_sizes)
```
**Gain**: Linear scalability with CPU count (8 cores → 8x speed)

### 2. SHA256 Hash Computation

#### Problem
Computing hashes for millions of tax codes requires many encoding and concatenation operations.

#### Implemented Solutions

**A. Pre-encoded Salt**
```python
# ❌ SLOW: Encode on each hash
SALT = "CF_ANPR_2025_SALT_KEY"
(SALT + tax_code).encode('utf-8')  # repeated encoding

# ✅ FAST: Pre-encoded salt
SALT_BYTES = SALT.encode('utf-8')  # encode once
```
**Gain**: ~10% faster hash computation

**B. Pre-allocated Bytearray**
```python
# ❌ SLOW: String concatenation
data = (SALT + tax_code).encode('utf-8')

# ✅ FAST: Pre-allocated bytearray
data = bytearray(len(SALT_BYTES) + len(tax_code))
data[:len(SALT_BYTES)] = SALT_BYTES
data[len(SALT_BYTES):] = tax_code.encode('utf-8')
```
**Gain**: ~5% faster hash computation, fewer memory allocations

### 3. KeyDB - Bulk Insert

#### Problem
Inserting millions of records one at a time is extremely slow due to network round-trips for each operation.

#### Implemented Solutions

**A. Pipeline with 10k Batches**
```python
# ❌ SLOW: Individual SETs
for hash, tax_code in batch:
    r.set(hash, tax_code)  # 1 round-trip per record

# ✅ FAST: Pipeline with batching
pipe = r.pipeline(transaction=False)  # transaction=False = no MULTI/EXEC
for hash, tax_code in batch:
    pipe.set(hash, tax_code)  # queued
pipe.execute()  # 1 round-trip for entire batch
```
**Gain**: 50-100x speed (10k operations in 1 round-trip vs 10k round-trips)

**B. transaction=False**
```python
# ❌ SLOWER: With MULTI/EXEC transaction
pipe = r.pipeline(transaction=True)

# ✅ FAST: Without transactional overhead
pipe = r.pipeline(transaction=False)
```
**Gain**: ~20-30% pipeline speed improvement

**C. Connection Pool**
```python
# ❌ SLOW: New connection per worker
r = redis.Redis(host=KEYDB_HOST, port=KEYDB_PORT)

# ✅ FAST: Global connection pool
_redis_pool = redis.ConnectionPool(
    host=KEYDB_HOST,
    max_connections=50,
    socket_keepalive=True
)
r = redis.Redis(connection_pool=_redis_pool)
```
**Gain**: ~15% speed improvement, reduced connection overhead

**D. Persistence Disabled During Insert**
```yaml
# docker-compose.yml
command: >
  --save ""              # Disable RDB snapshots
  --appendonly no        # Disable AOF
```
**Gain**: 2-3x faster insertion (no fsync to disk)

**Note**: Manual `SAVE` is executed after benchmark to persist data

### 4. PostgreSQL - Bulk Insert

#### Problem
Classic INSERT statements are slow for large volumes. Indexes further slow down insertions.

#### Implemented Solutions

**A. COPY FROM STDIN Instead of INSERT**
```python
# ❌ SLOW: INSERT with execute_values
execute_values(cur, "INSERT INTO codici_fiscali (codice_fiscale, hash) VALUES %s", batch)

# ✅ FAST: COPY FROM STDIN
buffer = io.StringIO()
for tax_code, hash in batch:
    buffer.write(f"{tax_code},{hash}\n")
buffer.seek(0)
cur.copy_expert(
    "COPY codici_fiscali (codice_fiscale, hash) FROM STDIN WITH (FORMAT CSV)",
    buffer
)
```
**Gain**: 2-5x faster insertion

**B. Drop Indexes Before, Recreate After**
```python
# PHASE 1: Drop indexes BEFORE bulk insert
cur.execute("DROP INDEX IF EXISTS idx_codici_fiscali_hash")
cur.execute("ALTER TABLE codici_fiscali DROP CONSTRAINT IF EXISTS codici_fiscali_codice_fiscale_key")

# PHASE 2: Bulk insertion (without index overhead)
# ... COPY FROM STDIN ...

# PHASE 3: Recreate indexes AFTER bulk insert
cur.execute("ALTER TABLE codici_fiscali ADD CONSTRAINT codici_fiscali_codice_fiscale_key UNIQUE (codice_fiscale)")
cur.execute("CREATE UNIQUE INDEX idx_codici_fiscali_hash ON codici_fiscali(hash)")
```
**Gain**: 3-10x faster insertion (depends on number of indexes)

**C. UNLOGGED TABLE**
```sql
CREATE UNLOGGED TABLE codici_fiscali (
    id SERIAL PRIMARY KEY,
    codice_fiscale VARCHAR(16) NOT NULL,
    hash VARCHAR(64) NOT NULL
);
```
**Gain**: 2-3x faster insertion (no WAL)

**⚠️ WARNING**: UNLOGGED TABLE loses data on PostgreSQL crash. Do not use in production for critical data.

**D. Optimized PostgreSQL Parameters**
```yaml
# docker-compose.yml
POSTGRES_INITDB_ARGS: >
  -c fsync=off
  -c synchronous_commit=off
  -c full_page_writes=off
```
```python
# main.py
cur.execute("SET maintenance_work_mem = '2GB'")  # For fast index creation
```
**Gain**: 1.5-2x overall speed improvement

**⚠️ WARNING**: fsync=off risks data corruption on crash. Benchmark only.

**E. ANALYZE After Bulk Insert**
```python
cur.execute("ANALYZE codici_fiscali")
```
**Benefit**: Updates query planner statistics for optimal future queries

**F. Client-side Pre-computed Hashes**
```python
# Compute hashes in Python before insertion
for tax_code in tax_code_list:
    hash_val = compute_sha256_hash(tax_code)
    buffer.write(f"{tax_code},{hash_val}\n")
```
**Benefits**:
- Distributes CPU load (PostgreSQL doesn't compute hashes)
- Reduces database CPU usage during bulk insert operations

### 5. PostgreSQL - Salt Rotation (BENCHMARK 4)

#### Problem
When you need to update the hashing salt for security reasons, you must recalculate all 6M+ hashes and update the database. Traditional UPDATE operations are extremely slow because:
- PostgreSQL must reorganize the PRIMARY KEY index (hash column) for every update
- Each UPDATE creates dead tuples (MVCC overhead)
- UPDATE with JOIN on millions of rows has significant overhead

**Naive UPDATE approach**: ~15,000 updates/sec (404 seconds for 6M records)

#### Implemented Solution

**TRUNCATE + COPY Strategy** (10-20x faster than UPDATE):

```python
# ❌ SLOW: UPDATE with JOIN (15k updates/sec)
UPDATE codici_fiscali
SET hash = new_hash
FROM temp_table
WHERE codici_fiscali.codice_fiscale = temp_table.codice_fiscale

# ✅ FAST: TRUNCATE + COPY (150k-200k inserts/sec)
# Step 1: Read existing tax codes from database
tax_codes = read_from_db()

# Step 2: Recalculate all hashes with NEW_SALT (parallel)
new_hashes = parallel_hash_computation(tax_codes, NEW_SALT)

# Step 3: Drop constraint
ALTER TABLE codici_fiscali DROP CONSTRAINT codici_fiscali_codice_fiscale_key

# Step 4: TRUNCATE table (instantaneous)
TRUNCATE TABLE codici_fiscali

# Step 5: Repopulate with COPY FROM STDIN (parallel, same as BENCHMARK 3)
COPY codici_fiscali (hash, codice_fiscale) FROM STDIN

# Step 6: Recreate constraint
ALTER TABLE codici_fiscali ADD CONSTRAINT codici_fiscali_codice_fiscale_key UNIQUE (codice_fiscale)
```

**Why This Works**:
1. **TRUNCATE is instantaneous** - just marks all pages as free (no row-by-row deletion)
2. **COPY is 10-20x faster** than UPDATE - optimized bulk loading path in PostgreSQL
3. **No MVCC overhead** - no dead tuples, no bloat
4. **No index reorganization during insert** - constraint is dropped, recreated at the end
5. **Parallel processing** - same multiprocessing strategy as BENCHMARK 3

**Trade-offs**:
- ✅ **Much faster**: 404s → ~30-40s (10x improvement)
- ✅ **No table bloat**: fresh data, no dead tuples
- ⚠️ **Requires downtime**: table is empty during repopulation
- ⚠️ **All-or-nothing**: can't update a subset of records

**Use Case**: Perfect for scheduled maintenance windows when you need to rotate salt keys for security compliance.

**Performance Comparison**:
```
UPDATE approach:  404.42s (14,836 updates/sec)
TRUNCATE + COPY:  ~30-40s (150,000-200,000 inserts/sec)
Speedup:          ~10-13x faster
```

### 6. Multiprocessing

#### Problem
Python GIL (Global Interpreter Lock) limits parallelism on CPU-intensive operations in threads.

#### Implemented Solution

**A. multiprocessing.Pool with map()**
```python
# ❌ SLOW: Single process
for batch in batches:
    write_postgres_batch_worker(batch)

# ✅ FAST: Parallelism with Pool
with Pool(processes=cpu_count()) as pool:
    results = pool.map(write_postgres_batch_worker, batches)
```
**Gain**: Linear scalability (8 CPUs = ~8x speed)

**B. Optimized Chunksize**
```python
chunksize = max(1, len(batches) // (num_processes * 4))
pool.map(worker, batches, chunksize=chunksize)
```
**Benefit**: Reduces IPC (Inter-Process Communication) overhead

---

## Database Structure

### PostgreSQL

#### Table `cf_raw` (Raw Tax Codes - UNLOGGED)
```sql
CREATE UNLOGGED TABLE cf_raw (
    codice_fiscale VARCHAR(16) PRIMARY KEY
);
```
**Purpose**: Stores raw Italian tax codes without hashes. Source data for benchmarks 2 and 3.

#### Table `codici_fiscali` (Tax Codes with Hashes - UNLOGGED)
```sql
CREATE UNLOGGED TABLE codici_fiscali (
    hash VARCHAR(64) PRIMARY KEY,
    codice_fiscale VARCHAR(16) NOT NULL
);
-- Index (created after bulk insert):
-- - UNIQUE constraint on codice_fiscale
```

**Structure**:
- `hash` (VARCHAR 64): SHA256 hash - PRIMARY KEY
- `codice_fiscale` (VARCHAR 16): Italian tax code - UNIQUE

**Hash**: Computed in Python (client-side) during insertion

### KeyDB

**Key-value structure**:
- Key: SHA256 hash (VARCHAR 64) - computed in Python
- Value: tax code (VARCHAR 16)

Example:
```
"a1b2c3d4..." → "RSSMRA80A01H501U"
```

---

## Why KeyDB?

KeyDB is a multithreaded fork of Redis that offers:
- Superior performance (up to 5x faster on multi-core workloads)
- 100% Redis compatibility (same protocol, same commands)
- Native multithreading (4 threads configured for max throughput)
- Dynamic configuration via `--server-threads` and `--server-thread-affinity`

---

## Parametric Configuration

### Available Environment Variables

The benchmark supports configuration via environment variables:

#### Python (main.py)

```bash
# PostgreSQL table type
USE_UNLOGGED_TABLE=false python main.py  # LOGGED (safe, slower) [default]
USE_UNLOGGED_TABLE=true python main.py   # UNLOGGED (fast, data loss risk)

# SHA256 hashing salts
CF_HASH_SALT="custom_salt_2025" python main.py      # Default: CF_ANPR_2025_SALT_KEY
NEW_SALT="new_salt_2026" python main.py             # For BENCHMARK 4 (salt rotation)

# KeyDB connections
KEYDB_HOST=redis.example.com python main.py   # Default: localhost
KEYDB_PORT=6380 python main.py                # Default: 6379
KEYDB_DB=1 python main.py                     # Default: 0

# PostgreSQL connections
POSTGRES_HOST=pg.example.com python main.py   # Default: localhost
POSTGRES_PORT=5433 python main.py             # Default: 5432
POSTGRES_DB=cf_benchmark python main.py       # Default: cf_benchmark
POSTGRES_USER=myuser python main.py           # Default: postgres
POSTGRES_PASSWORD=mypass python main.py       # Default: postgres

# Benchmark parameters
TOTAL_IDS=1000000 python main.py                      # Default: 6000000
BATCH_SIZE_COMPUTATION=50000 python main.py           # Default: 100000
BATCH_SIZE_CF_RAW=25000 python main.py                # Default: 50000
BATCH_SIZE_KEYDB=5000 python main.py                  # Default: 10000
BATCH_SIZE_POSTGRES=25000 python main.py              # Default: 50000
BATCH_SIZE_POSTGRES_UPDATE=25000 python main.py       # Default: 50000

# Enable/disable individual benchmarks
RUN_POSTGRES_CF_WRITE=false python main.py      # Default: true (BENCHMARK 1bis)
RUN_KEYDB=false python main.py                  # Default: true (BENCHMARK 2)
RUN_POSTGRES_INSERT=false python main.py        # Default: true (BENCHMARK 3)
RUN_POSTGRES_SALT_UPDATE=false python main.py   # Default: true (BENCHMARK 4 - salt rotation)

# Combined example: custom production configuration
USE_UNLOGGED_TABLE=false \
POSTGRES_HOST=pg.prod.example.com \
POSTGRES_USER=cf_user \
POSTGRES_PASSWORD=secure_password \
CF_HASH_SALT="production_salt_2025" \
python main.py

# Example: Quick test with 100k records, KeyDB only
TOTAL_IDS=100000 \
RUN_POSTGRES_CF_WRITE=false \
RUN_POSTGRES_INSERT=false \
python main.py

# Example: Test salt rotation (BENCHMARK 4 only)
# Note: Requires existing data in codici_fiscali table (run BENCHMARK 3 first)
# Uses TRUNCATE + COPY strategy (10x faster than UPDATE)
RUN_POSTGRES_CF_WRITE=false \
RUN_KEYDB=false \
RUN_POSTGRES_INSERT=false \
RUN_POSTGRES_SALT_UPDATE=true \
NEW_SALT="CF_ANPR_2026_NEW_SALT" \
python main.py

# Example: Full pipeline including salt rotation
# Runs all benchmarks in sequence: generate → write → KeyDB → PostgreSQL → salt rotation
python main.py
```

**Notes**:
- Code automatically detects current table type
- If needed, converts table from LOGGED ↔ UNLOGGED before benchmark
- Conversion time is proportional to table size
- All variables have reasonable defaults for local environment
- Salt is configurable for different environments (dev/staging/prod)
- Benchmark parameters allow customization of dataset size and batch sizes
- Individual benchmarks can be enabled/disabled via flags

#### Docker Compose

```bash
# KeyDB
export KEYDB_MEMORY=16gb
export KEYDB_THREADS=8

# PostgreSQL
export PG_SHARED_BUFFERS=8GB
export PG_WORK_MEM=256MB
export PG_MAINTENANCE_WORK_MEM=2GB
export PG_EFFECTIVE_CACHE_SIZE=16GB
export PG_MAX_WAL_SIZE=4GB

# Start containers with custom configuration
docker-compose up -d
```

The `run_benchmark.sh` script automatically configures these parameters based on available resources.

## Quick Test

For a quick test with 100k records instead of 6 million:

```bash
# Using environment variable (recommended)
TOTAL_IDS=100000 python main.py

# Or run only specific benchmarks (e.g., only KeyDB)
TOTAL_IDS=100000 RUN_POSTGRES_INSERT=false python main.py
```

---

## Usage Examples

### Test with UNLOGGED TABLE (Maximum Performance)

```bash
# 1. Launch benchmark configuration
docker-compose up -d

# 2. Execute benchmark with UNLOGGED
python main.py

# 3. Verify performance
# You should see ~3-5x speed vs LOGGED
```

### Test with LOGGED TABLE (Production)

```bash
# 1. Launch production configuration
docker-compose -f docker-compose.production.yml up -d

# 2. Execute benchmark with LOGGED
USE_UNLOGGED_TABLE=false python main.py

# 3. Compare performance
# Slower insertion but data safe on crash
```

### A/B Comparison (UNLOGGED vs LOGGED)

```bash
# Test 1: UNLOGGED (fast)
docker-compose up -d
python main.py > results_unlogged.txt

# Clean database
docker-compose down -v
docker-compose up -d

# Test 2: LOGGED (safe)
USE_UNLOGGED_TABLE=false python main.py > results_logged.txt

# Compare results
diff results_unlogged.txt results_logged.txt
```

### Test with fsync=on (Production)

```bash
# 1. Use production configuration (fsync=on)
docker-compose -f docker-compose.production.yml up -d

# 2. Execute with LOGGED TABLE
USE_UNLOGGED_TABLE=false python main.py

# Result: safest configuration but slower
```

---

## Cleanup

```bash
# Stop containers
docker-compose down

# Stop containers and delete data
docker-compose down -v

# Stop production configuration
docker-compose -f docker-compose.production.yml down -v
```

---

## Important Production Notes

**⚠️ WARNING**: This project is optimized for **maximum performance benchmarking**.

The following practices are **NOT safe for production environments**:

1. **UNLOGGED TABLE**: Data is lost on PostgreSQL crash
2. **fsync=off**: Risk of data corruption
3. **KeyDB without persistence**: Data lost on restart
4. **Hardcoded credentials**: Passwords in plain text in code

**For production**:
- Use normal LOGGED tables
- Enable fsync and synchronous_commit
- Enable AOF on KeyDB (`--appendonly yes`)
- Use environment variables for credentials
- Implement automated backups
- Add monitoring and alerting
- Implement retry logic and robust error handling

Bulk insert optimization practices (COPY FROM STDIN, temporary index removal, Redis Pipeline) are valid for production during scheduled ETL operations.
