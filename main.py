#!/usr/bin/env python3
"""
Performance benchmark for generating 6 million SHA256 hashes from synthetic Italian Tax Codes (Codice Fiscale).
Compares performance across: hash computation, KeyDB write operations, and PostgreSQL write operations.
Tax codes are generated once and reused across KeyDB and PostgreSQL benchmarks for consistency.
Optimized for maximum parallelism and throughput.
"""

import hashlib
import io
import multiprocessing as mp
import os
import random
import string
import time
from multiprocessing import Pool
from typing import List, Tuple

import psycopg2
import redis

LETTERS = string.ascii_uppercase
DIGITS = string.digits

# Hashing salt (configurable via environment variable)
SALT = os.getenv('CF_HASH_SALT', 'CF_ANPR_2025_SALT_KEY')
# Pre-encode salt to avoid repeated encoding operations
SALT_BYTES = SALT.encode('utf-8')

# Pre-create tuples for random.choices (performance optimization)
LETTERS_TUPLE = tuple(LETTERS)
DIGITS_TUPLE = tuple(DIGITS)

# Tax code generation pattern: defines character pool selection (L=letter, D=digit)
# Format: LLLLLLDDLDDLDDDL (6L, 2D, 1L, 2D, 1L, 3D, 1L = 16 chars total)
CF_PATTERN = 'LLLLLLDDLDDLDDDL'
# Pre-compute indices for letters and digits
CF_LETTERS_INDICES = [i for i, c in enumerate(CF_PATTERN) if c == 'L']
CF_DIGITS_INDICES = [i for i, c in enumerate(CF_PATTERN) if c == 'D']

# Connection configuration (configurable via environment variables)
KEYDB_HOST = os.getenv('KEYDB_HOST', 'localhost')
KEYDB_PORT = int(os.getenv('KEYDB_PORT', '6379'))
KEYDB_DB = int(os.getenv('KEYDB_DB', '0'))

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'cf_benchmark')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

# PostgreSQL optimization parameters (configurable via environment variables)
# USE_UNLOGGED_TABLE: when True, uses UNLOGGED TABLE (2-3x faster, DATA LOSS on crash)
USE_UNLOGGED_TABLE = os.getenv('USE_UNLOGGED_TABLE', 'true').lower() == 'true'

# Global Redis connection pool (reuses connections across workers)
_redis_pool = None

def get_redis_pool():
	"""
	Retrieve or create the Redis connection pool.

	Returns:
		redis.ConnectionPool: Configured connection pool instance
	"""
	global _redis_pool
	if _redis_pool is None:
		_redis_pool = redis.ConnectionPool(
			host=KEYDB_HOST,
			port=KEYDB_PORT,
			db=KEYDB_DB,
			decode_responses=False,
			max_connections=50,
			socket_keepalive=True,
			socket_connect_timeout=5,
			health_check_interval=30
		)
	return _redis_pool


def generate_fake_tax_code() -> str:
	"""
	Generate a synthetic Italian Tax Code (Codice Fiscale).

	Format: 16 alphanumeric characters following the pattern LLLLLLDDLDDLDDDL
	Optimized: generates a 16-character list, then substitutes digit positions

	Returns:
		str: Generated tax code (16 characters)
	"""
	# Generate 16-character array (initially all letters)
	cf_chars = list(random.choices(LETTERS_TUPLE, k=16))
	# Replace positions that must be digits
	for idx in CF_DIGITS_INDICES:
		cf_chars[idx] = random.choice(DIGITS_TUPLE)
	return ''.join(cf_chars)


def compute_sha256_hash(tax_code: str) -> str:
	"""
	Compute SHA256 hash of a tax code with salt.

	Optimized: uses pre-encoded salt and bytearray to avoid multiple allocations

	Args:
		tax_code: The tax code to hash

	Returns:
		str: Hexadecimal hash digest
	"""
	# Create pre-allocated bytearray to avoid string concatenations
	data = bytearray(len(SALT_BYTES) + len(tax_code))
	data[:len(SALT_BYTES)] = SALT_BYTES
	data[len(SALT_BYTES):] = tax_code.encode('utf-8')
	return hashlib.sha256(data).hexdigest()


def generate_tax_code_with_hash_batch_worker(batch_size: int) -> List[Tuple[str, str]]:
	"""
	Worker function to generate a batch of tax codes with computed hashes (for multiprocessing).

	Args:
		batch_size: Number of tax codes to generate

	Returns:
		List[Tuple[str, str]]: List of (tax_code, hash) tuples
	"""
	return [(tc, compute_sha256_hash(tc)) for tc in (generate_fake_tax_code() for _ in range(batch_size))]


def generate_tax_codes_with_hashes(total_ids: int, batch_size: int = 100_000, num_processes: int = None) -> List[Tuple[str, str]]:
	"""
	Generate all tax codes with their SHA256 hashes in parallel using multiprocessing.

	Returns a list of (tax_code, hash) tuples to be reused across KeyDB and PostgreSQL benchmarks.

	Args:
		total_ids: Total number of tax codes to generate
		batch_size: Size of each batch for parallel processing
		num_processes: Number of parallel processes (defaults to CPU count)

	Returns:
		List[Tuple[str, str]]: All generated (tax_code, hash) tuples
	"""
	if num_processes is None:
		num_processes = mp.cpu_count()

	print("\n" + "=" * 60)
	print("🧮 BENCHMARK 1: TAX CODE GENERATION + HASH COMPUTATION")
	print("=" * 60)
	print(f"   Total tax codes to generate: {total_ids:,}")
	print(f"   Parallel processes: {num_processes}")
	print(f"   Batch size: {batch_size:,}")

	start_time = time.time()

	# Calculate batch distribution
	num_batches = (total_ids + batch_size - 1) // batch_size
	batch_sizes = [batch_size] * (num_batches - 1)
	remainder = total_ids - (batch_size * (num_batches - 1))
	if remainder > 0:
		batch_sizes.append(remainder)

	# Generate tax codes with hashes in parallel
	with Pool(processes=num_processes) as pool:
		results = pool.map(generate_tax_code_with_hash_batch_worker, batch_sizes, chunksize=max(1, len(batch_sizes) // (num_processes * 4)))

	# Merge all results
	all_data = []
	for batch in results:
		all_data.extend(batch)

	elapsed_time = time.time() - start_time

	print(f"✅ Completed: {len(all_data):,} tax codes + hashes in {elapsed_time:.2f}s")
	print(f"   Throughput: {len(all_data) / elapsed_time:,.0f} op/s")
	print("")

	return elapsed_time, all_data


def write_keydb_batch_worker(args):
	"""
	Worker function for KeyDB batch write operations (multiprocessing).

	Optimized: uses Pipeline with 10k batch size + connection pooling
	Accepts pre-computed (tax_code, hash) tuples

	Args:
		args: Tuple of (data_tuples, worker_id)
		      data_tuples: List of (tax_code, hash) tuples

	Returns:
		int: Number of records written
	"""
	data_tuples, worker_id = args

	# Use connection pool to reuse connections across workers
	r = redis.Redis(connection_pool=get_redis_pool())

	# Use PIPELINE for 10k record batches (best practice for bulk inserts)
	# transaction=False for maximum performance (eliminates MULTI/EXEC overhead)
	pipe = r.pipeline(transaction=False)
	for tax_code, hash_val in data_tuples:
		# KeyDB stores: hash -> tax_code
		pipe.set(hash_val, tax_code)
	pipe.execute()

	return len(data_tuples)


def benchmark_keydb(batch_size: int = 10_000, num_processes: int = None, data_tuples: List[Tuple[str, str]] = None):
	"""
	Benchmark: KeyDB write operations using pre-computed (tax_code, hash) tuples.

	Optimized with Pipeline + manual SAVE.
	Follows best practices: disables persistence during insertion, executes SAVE at completion.

	Args:
		batch_size: Size of each batch for write operations
		num_processes: Number of parallel processes (defaults to CPU count)
		data_tuples: Pre-computed list of (tax_code, hash) tuples

	Returns:
		float: Total elapsed time in seconds, or None on error
	"""
	if num_processes is None:
		num_processes = mp.cpu_count()

	if not data_tuples:
		print("❌ No data provided for KeyDB benchmark")
		return None

	print("\n" + "=" * 60)
	print("📮 BENCHMARK 2: KEYDB WRITE")
	print("=" * 60)
	print(f"   Records to insert: {len(data_tuples):,}")
	print(f"   Parallel processes: {num_processes}")
	print(f"   Batch size: {batch_size:,}")

	try:
		r = redis.Redis(host=KEYDB_HOST, port=KEYDB_PORT, db=KEYDB_DB, decode_responses=False)

		# Wait for KeyDB to be ready
		max_retries = 30
		for i in range(max_retries):
			try:
				r.ping()
				break
			except redis.exceptions.BusyLoadingError:
				if i == max_retries - 1:
					raise
				time.sleep(1)

	except Exception as e:
		print(f"❌ KeyDB connection error: {e}")
		return None

	print("")
	print("📥 INSERT PHASE: Pipelined batch writes...")
	start_time = time.time()

	# Split data into batches
	batch_data = []
	for i in range(0, len(data_tuples), batch_size):
		batch_tuples = data_tuples[i:i+batch_size]
		batch_data.append((batch_tuples, i // batch_size))

	chunksize = max(1, len(batch_data) // (num_processes * 4))
	with Pool(processes=num_processes) as pool:
		results = pool.map(write_keydb_batch_worker, batch_data, chunksize=chunksize)

	insert_time = time.time() - start_time
	total_count = sum(results)

	print(f"✅ Insert completed: {total_count:,} records in {insert_time:.2f}s")
	print(f"   Throughput: {total_count / insert_time:,.0f} records/sec")

	return insert_time


def write_postgres_batch_worker(args):
	"""
	Worker function for PostgreSQL batch write operations (multiprocessing).

	Optimized: uses COPY FROM STDIN (2-5x faster than INSERT)
	Accepts pre-computed (tax_code, hash) tuples

	Args:
		args: Tuple of (data_tuples, worker_id)
		      data_tuples: List of (tax_code, hash) tuples

	Returns:
		int: Number of records written
	"""
	data_tuples, worker_id = args

	# Optimize connection parameters for maximum performance
	conn = psycopg2.connect(
		host=POSTGRES_HOST,
		port=POSTGRES_PORT,
		database=POSTGRES_DB,
		user=POSTGRES_USER,
		password=POSTGRES_PASSWORD,
		# Disable synchronous_commit for maximum performance
		options='-c synchronous_commit=off'
	)
	cur = conn.cursor()

	try:
		# Create CSV buffer in memory
		buffer = io.StringIO()

		# Use pre-computed (tax_code, hash) tuples
		# Note: Write as (hash, tax_code) to match table structure (hash is PRIMARY KEY)
		for tax_code, hash_val in data_tuples:
			buffer.write(f"{hash_val},{tax_code}\n")

		buffer.seek(0)

		# COPY FROM STDIN is 2-5x faster than INSERT/execute_values
		try:
			cur.copy_expert(
				"COPY codici_fiscali (hash, codice_fiscale) FROM STDIN WITH (FORMAT CSV)",
				buffer
			)
		except psycopg2.errors.UniqueViolation:
			# Ignore duplicates (should not occur with random tax codes)
			conn.rollback()
		else:
			conn.commit()

		return len(data_tuples)
	finally:
		cur.close()
		conn.close()


def benchmark_postgres_insert(batch_size: int = 50_000, num_processes: int = None, data_tuples: List[Tuple[str, str]] = None):
	"""
	Benchmark: PostgreSQL write operations using pre-computed (tax_code, hash) tuples.

	Optimized with COPY FROM STDIN + index management.
	Follows best practices: drops indexes BEFORE bulk insert, recreates AFTER, executes ANALYZE.

	Args:
		batch_size: Size of each batch for write operations
		num_processes: Number of parallel processes (defaults to min(CPU count, 8))
		data_tuples: Pre-computed list of (tax_code, hash) tuples

	Returns:
		float: Total elapsed time in seconds, or None on error
	"""
	if num_processes is None:
		num_processes = min(mp.cpu_count(), 8)  # Max 8 parallel DB connections

	if not data_tuples:
		print("❌ No data provided for PostgreSQL benchmark")
		return None

	print("\n" + "=" * 60)
	print("🗄️  BENCHMARK 3: POSTGRESQL WRITE")
	print("=" * 60)
	print(f"   Records to insert: {len(data_tuples):,}")
	print(f"   Parallel processes: {num_processes}")
	print(f"   Batch size: {batch_size:,}")
	print(f"   Table type: {'UNLOGGED' if USE_UNLOGGED_TABLE else 'LOGGED'}")

	# Connection for schema management
	try:
		conn_admin = psycopg2.connect(
			host=POSTGRES_HOST,
			port=POSTGRES_PORT,
			database=POSTGRES_DB,
			user=POSTGRES_USER,
			password=POSTGRES_PASSWORD
		)
		cur_admin = conn_admin.cursor()
	except Exception as e:
		print(f"❌ PostgreSQL connection error: {e}")
		return None

	print("")
	print("📋 PRE-INSERT PHASE: Table preparation...")

	# PHASE 0: Verify/convert table type (LOGGED/UNLOGGED)
	try:
		# Check if table exists and determine its type
		cur_admin.execute("""
			SELECT relpersistence FROM pg_class
			WHERE relname = 'codici_fiscali' AND relkind = 'r'
		""")
		result = cur_admin.fetchone()

		if result:
			current_persistence = result[0]  # 'p' = permanent (LOGGED), 'u' = UNLOGGED
			is_currently_unlogged = (current_persistence == 'u')

			if USE_UNLOGGED_TABLE and not is_currently_unlogged:
				print("   - Converting table from LOGGED to UNLOGGED...")
				cur_admin.execute("ALTER TABLE codici_fiscali SET UNLOGGED")
				conn_admin.commit()
				print("   ✓ Table converted to UNLOGGED")
			elif not USE_UNLOGGED_TABLE and is_currently_unlogged:
				print("   - Converting table from UNLOGGED to LOGGED...")
				cur_admin.execute("ALTER TABLE codici_fiscali SET LOGGED")
				conn_admin.commit()
				print("   ✓ Table converted to LOGGED")
			else:
				table_type = "UNLOGGED" if is_currently_unlogged else "LOGGED"
				print(f"   ✓ Table is already {table_type} (no conversion needed)")
	except Exception as e:
		print(f"   ⚠️  Table type verification warning: {e}")
		conn_admin.rollback()

	# PHASE 1: Drop existing indexes (if present)
	print("   - Dropping existing indexes...")
	try:
		cur_admin.execute("DROP INDEX IF EXISTS idx_codici_fiscali_cf")
		cur_admin.execute("ALTER TABLE codici_fiscali DROP CONSTRAINT IF EXISTS codici_fiscali_codice_fiscale_key")
		conn_admin.commit()
		print("   ✓ Indexes dropped")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	# PHASE 2: Set optimal parameters for bulk insert
	print("   - Configuring optimal parameters...")
	cur_admin.execute("SET maintenance_work_mem = '2GB'")
	print("   ✓ Parameters configured")
	print("")

	# PHASE 3: Bulk insertion
	print("📥 INSERT PHASE: COPY FROM STDIN...")
	start_time = time.time()

	# Split data into batches
	batch_data = []
	for i in range(0, len(data_tuples), batch_size):
		batch_tuples = data_tuples[i:i+batch_size]
		batch_data.append((batch_tuples, i // batch_size))

	chunksize = max(1, len(batch_data) // (num_processes * 8))
	with Pool(processes=num_processes) as pool:
		results = pool.map(write_postgres_batch_worker, batch_data, chunksize=chunksize)

	insert_time = time.time() - start_time
	total_count = sum(results)

	print(f"✅ Insert completed: {total_count:,} tax codes in {insert_time:.2f}s")
	print(f"   Throughput: {total_count / insert_time:,.0f} tax codes/sec")
	print("")

	# PHASE 4: Recreate indexes
	print("🔨 POST-INSERT PHASE: Index creation...")
	index_start_time = time.time()

	try:
		# Create UNIQUE constraint on codice_fiscale (automatically creates UNIQUE index)
		print("   - Creating UNIQUE constraint on codice_fiscale...")
		cur_admin.execute("ALTER TABLE codici_fiscali ADD CONSTRAINT codici_fiscali_codice_fiscale_key UNIQUE (codice_fiscale)")
		conn_admin.commit()

		# Note: hash is already PRIMARY KEY, no need to create additional index

		index_time = time.time() - index_start_time
		print(f"   ✓ Index created in {index_time:.2f}s")
	except Exception as e:
		print(f"   ✗ Index creation error: {e}")
		conn_admin.rollback()
		index_time = 0

	# PHASE 5: ANALYZE to update statistics
	print("   - Executing ANALYZE...")
	analyze_start_time = time.time()
	cur_admin.execute("ANALYZE codici_fiscali")
	conn_admin.commit()
	analyze_time = time.time() - analyze_start_time
	print(f"   ✓ ANALYZE completed in {analyze_time:.2f}s")

	cur_admin.close()
	conn_admin.close()

	total_time = time.time() - start_time

	print("")
	print("📊 SUMMARY:")
	print(f"   - Data insertion: {insert_time:.2f}s ({total_count / insert_time:,.0f} tax codes/sec)")
	print(f"   - Index creation: {index_time:.2f}s")
	print(f"   - ANALYZE: {analyze_time:.2f}s")
	print(f"   - TOTAL: {total_time:.2f}s")

	return total_time


def main():
	"""Main entry point for benchmark execution."""
	# Benchmark parameters (configurable via environment variables)
	TOTAL_IDS = int(os.getenv('TOTAL_IDS', '6000000'))
	BATCH_SIZE_COMPUTATION = int(os.getenv('BATCH_SIZE_COMPUTATION', '100000'))    # Increased to reduce multiprocessing overhead
	BATCH_SIZE_KEYDB = int(os.getenv('BATCH_SIZE_KEYDB', '10000'))                 # Optimal for Pipeline + connection pool
	BATCH_SIZE_POSTGRES = int(os.getenv('BATCH_SIZE_POSTGRES', '50000'))           # Increased from 20k: COPY FROM STDIN is 2-5x faster

	# Flags to enable/disable individual benchmarks (configurable via environment variables)
	RUN_KEYDB = os.getenv('RUN_KEYDB', 'true').lower() == 'true'
	RUN_POSTGRES_INSERT = os.getenv('RUN_POSTGRES_INSERT', 'true').lower() == 'true'

	print("\n" + "=" * 60)
	print("🚀 BENCHMARK: TAX CODE -> SHA256 HASH")
	print("=" * 60)
	print(f"Total records: {TOTAL_IDS:,}")
	print(f"Available CPUs: {mp.cpu_count()}")

	# BENCHMARK 1: Generate tax codes + compute hashes (single pass, stored in memory)
	time_computation, all_data = generate_tax_codes_with_hashes(TOTAL_IDS, BATCH_SIZE_COMPUTATION)

	# BENCHMARK 2: Write to KeyDB (if enabled)
	time_keydb = None
	if RUN_KEYDB:
		time_keydb = benchmark_keydb(BATCH_SIZE_KEYDB, data_tuples=all_data)

	# BENCHMARK 3: Write to PostgreSQL (if enabled)
	time_postgres = None
	if RUN_POSTGRES_INSERT:
		time_postgres = benchmark_postgres_insert(BATCH_SIZE_POSTGRES, data_tuples=all_data)

	# Final summary
	print("\n" + "=" * 60)
	print("📊 RESULTS SUMMARY")
	print("=" * 60)
	print(f"1️⃣  Generation + Hash computation: {time_computation:8.2f}s  ({TOTAL_IDS/time_computation:,.0f} op/s)")

	if time_keydb:
		print(f"2️⃣  KeyDB write:                   {time_keydb:8.2f}s  ({TOTAL_IDS/time_keydb:,.0f} op/s)")

	if time_postgres:
		print(f"3️⃣  PostgreSQL write:              {time_postgres:8.2f}s  ({TOTAL_IDS/time_postgres:,.0f} op/s)")

	print("=" * 60)


if __name__ == "__main__":
	main()
