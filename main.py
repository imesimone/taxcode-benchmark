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
from typing import List, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from dotenv import load_dotenv
import psycopg2
import redis

# Load environment variables from .env file
load_dotenv()

LETTERS = string.ascii_uppercase
DIGITS = string.digits

# Hashing salt (configurable via environment variable)
SALT = os.getenv('CF_HASH_SALT', 'CF_ANPR_2025_SALT_KEY')
# Pre-encode salt to avoid repeated encoding operations
SALT_BYTES = SALT.encode('utf-8')

# New salt for BENCHMARK 4 (salt rotation/update scenario)
NEW_SALT = os.getenv('NEW_SALT', 'CF_ANPR_2026_NEW_SALT')
NEW_SALT_BYTES = NEW_SALT.encode('utf-8')

# Pre-create tuples for random.choices (performance optimization)
LETTERS_TUPLE = tuple(LETTERS)
DIGITS_TUPLE = tuple(DIGITS)

# Tax code generation pattern: defines character pool selection (L=letter, D=digit)
# Format: LLLLLLDDLDDLDDDL (6L, 2D, 1L, 2D, 1L, 3D, 1L = 16 chars total)
CF_PATTERN = 'LLLLLLDDLDDLDDDL'
# Pre-compute indices for letters and digits
CF_LETTERS_INDICES = [i for i, c in enumerate(CF_PATTERN) if c == 'L']
CF_DIGITS_INDICES = [i for i, c in enumerate(CF_PATTERN) if c == 'D']

# Pre-allocated buffer for SHA256 hash computation (optimization)
# Tax code is always 16 characters, so buffer size is fixed
_HASH_BUFFER = bytearray(len(SALT.encode('utf-8')) + 16)
_HASH_BUFFER[:len(SALT.encode('utf-8'))] = SALT.encode('utf-8')

# Connection configuration (configurable via environment variables)
KEYDB_HOST = os.getenv('KEYDB_HOST', 'localhost')
KEYDB_PORT = int(os.getenv('KEYDB_PORT', '6379'))
KEYDB_DB = int(os.getenv('KEYDB_DB', '0'))

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'cf_benchmark')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

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

	Optimized: uses pre-allocated global buffer to avoid memory allocations

	Args:
		tax_code: The tax code to hash

	Returns:
		str: Hexadecimal hash digest
	"""
	# Use pre-allocated global buffer (tax code is always 16 chars)
	_HASH_BUFFER[len(SALT_BYTES):] = tax_code.encode('utf-8')
	return hashlib.sha256(_HASH_BUFFER).hexdigest()


def generate_tax_code_with_hash_batch_worker(batch_size: int) -> List[Tuple[str, str]]:
	"""
	Worker function to generate a batch of tax codes with computed hashes (for multiprocessing).

	Args:
		batch_size: Number of tax codes to generate

	Returns:
		List[Tuple[str, str]]: List of (tax_code, hash) tuples
	"""
	return [(tc, compute_sha256_hash(tc)) for tc in (generate_fake_tax_code() for _ in range(batch_size))]


def generate_tax_codes_with_hashes(total_ids: int, batch_size: int = 100_000, num_processes: int = None) -> Tuple[float, List[Tuple[str, str]]]:
	"""
	Generate all tax codes with their SHA256 hashes in parallel using multiprocessing.

	Returns a list of (tax_code, hash) tuples to be reused across KeyDB and PostgreSQL benchmarks.

	Args:
		total_ids: Total number of tax codes to generate
		batch_size: Size of each batch for parallel processing
		num_processes: Number of parallel processes (defaults to CPU count)

	Returns:
		Tuple[float, List[Tuple[str, str]]]: (elapsed_time, all_data)
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
	with mp.Pool(processes=num_processes) as pool:
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


def generate_cf_batch_worker(size):
	"""
	Worker function for generating tax codes (multiprocessing).

	Args:
		size: Number of tax codes to generate

	Returns:
		List[str]: List of generated tax codes
	"""
	return [generate_fake_tax_code() for _ in range(size)]


def generate_tax_codes_only(total_ids: int, batch_size: int = 100_000, num_processes: int = None) -> Tuple[float, List[str]]:
	"""
	Generate tax codes WITHOUT computing hashes (for BENCHMARK 1).

	Args:
		total_ids: Total number of tax codes to generate
		batch_size: Size of each batch for parallel processing
		num_processes: Number of parallel processes (defaults to CPU count)

	Returns:
		Tuple[float, List[str]]: (elapsed_time, all_tax_codes)
	"""
	if num_processes is None:
		num_processes = mp.cpu_count()

	print("\n" + "=" * 60)
	print("🧮 BENCHMARK 1: TAX CODE GENERATION (no hashing)")
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

	# Generate tax codes in parallel
	with mp.Pool(processes=num_processes) as pool:
		results = pool.map(generate_cf_batch_worker, batch_sizes, chunksize=max(1, len(batch_sizes) // (num_processes * 4)))

	# Merge all results
	all_tax_codes = []
	for batch in results:
		all_tax_codes.extend(batch)

	elapsed_time = time.time() - start_time

	print(f"✅ Completed: {len(all_tax_codes):,} tax codes in {elapsed_time:.2f}s")
	print(f"   Throughput: {len(all_tax_codes) / elapsed_time:,.0f} op/s")
	print("")

	return elapsed_time, all_tax_codes


# NumPy arrays for fast tax code generation
LETTERS_ARRAY = np.array(list(LETTERS_TUPLE))
DIGITS_ARRAY = np.array(list(DIGITS_TUPLE))


def _pin_numpy_threads():
	"""
	Limit internal BLAS/OpenMP threads to avoid oversubscription
	when launching multiple processes.
	"""
	os.environ.setdefault("OMP_NUM_THREADS", "1")
	os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
	os.environ.setdefault("MKL_NUM_THREADS", "1")
	os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")


def _gen_block_numpy(count: int, child_seed: Optional[int]) -> List[str]:
	"""
	Generate `count` tax codes in a single process using NumPy.

	Args:
		count: Number of tax codes to generate
		child_seed: Seed for reproducibility (None for random)

	Returns:
		List[str]: List of generated tax codes
	"""
	rng = np.random.default_rng(child_seed)

	# Generate matrix: LLLLLLDDLDDLDDDL (6L, 2D, 1L, 2D, 1L, 3D, 1L)
	mat = np.concatenate((
		rng.choice(LETTERS_ARRAY, size=(count, 6), shuffle=False),
		rng.choice(DIGITS_ARRAY, size=(count, 2), shuffle=False),
		rng.choice(LETTERS_ARRAY, size=(count, 1), shuffle=False),
		rng.choice(DIGITS_ARRAY, size=(count, 2), shuffle=False),
		rng.choice(LETTERS_ARRAY, size=(count, 1), shuffle=False),
		rng.choice(DIGITS_ARRAY, size=(count, 3), shuffle=False),
		rng.choice(LETTERS_ARRAY, size=(count, 1), shuffle=False)
	), axis=1)

	# Convert each row to string (e.g., "ABCDEF12G34H567I")
	return [''.join(row.tolist()) for row in mat]


def generate_tax_codes_only_numpy(
	total_ids: int,
	seed: Optional[int] = None,
	workers: Optional[int] = None,
	min_block: int = 100_000
) -> Tuple[float, List[str]]:
	"""
	Generate tax codes WITHOUT hashing using NumPy (parallel version).

	Much faster than pure Python for large datasets.
	Uses ProcessPoolExecutor for deterministic parallel execution.

	Args:
		total_ids: Total number of tax codes to generate
		seed: Random seed for reproducibility (None for random)
		workers: Number of parallel workers (defaults to CPU count)
		min_block: Minimum block size for parallelization

	Returns:
		Tuple[float, List[str]]: (elapsed_time, all_tax_codes)
	"""
	if workers is None:
		workers = max(1, (os.cpu_count() or 1))

	print("\n" + "=" * 60)
	print("🧮 BENCHMARK 1: TAX CODE GENERATION (NumPy parallel)")
	print("=" * 60)
	print(f"   Total tax codes to generate: {total_ids:,}")
	print(f"   Workers: {workers}")
	print("")

	start_time = time.time()

	# Divide into blocks (not too small to avoid IPC overhead)
	if total_ids <= min_block:
		blocks = [total_ids]
	else:
		nb = max(workers, total_ids // min_block)
		base = total_ids // nb
		rem = total_ids % nb
		blocks = [base + (1 if i < rem else 0) for i in range(nb)]
		blocks = [b for b in blocks if b > 0]

	# Generate deterministic seeds for each block
	child_seeds: List[Optional[int]]
	if seed is None:
		child_seeds = [None] * len(blocks)
	else:
		ss = np.random.SeedSequence(seed)
		spawned = ss.spawn(len(blocks))
		child_seeds = [int(s.generate_state(1)[0]) for s in spawned]

	# Parallel execution
	results: List[str] = []
	with ProcessPoolExecutor(max_workers=workers, initializer=_pin_numpy_threads) as ex:
		futures = [ex.submit(_gen_block_numpy, cnt, cs) for cnt, cs in zip(blocks, child_seeds)]
		for fut in as_completed(futures):
			block_res = fut.result()
			results.extend(block_res)

	elapsed_time = time.time() - start_time

	print(f"✅ Completed: {len(results):,} tax codes in {elapsed_time:.2f}s")
	print(f"   Throughput: {len(results) / max(elapsed_time, 1e-9):,.0f} op/s\n")

	return elapsed_time, results


def write_cf_raw_batch_worker(args):
	"""
	Worker function for writing raw tax codes to PostgreSQL cf_raw table.

	Args:
		args: Tuple of (tax_codes_batch, worker_id)

	Returns:
		int: Number of records written
	"""
	tax_codes_batch, worker_id = args

	conn = psycopg2.connect(
		host=POSTGRES_HOST,
		port=POSTGRES_PORT,
		database=POSTGRES_DB,
		user=POSTGRES_USER,
		password=POSTGRES_PASSWORD,
		options='-c synchronous_commit=off'
	)
	cur = conn.cursor()

	try:
		# Create CSV buffer in memory
		buffer = io.StringIO()
		for tax_code in tax_codes_batch:
			buffer.write(f"{tax_code}\n")
		buffer.seek(0)

		# COPY FROM STDIN is 2-5x faster than INSERT
		try:
			cur.copy_expert(
				"COPY cf_raw (codice_fiscale) FROM STDIN WITH (FORMAT CSV)",
				buffer
			)
		except psycopg2.errors.UniqueViolation:
			# Ignore duplicates
			conn.rollback()
		else:
			conn.commit()

		return len(tax_codes_batch)
	finally:
		cur.close()
		conn.close()


def benchmark_postgres_cf_raw_write(batch_size: int = 50_000, num_processes: int = None, tax_codes: List[str] = None) -> Optional[float]:
	"""
	BENCHMARK 1bis: Write raw tax codes (no hashes) to PostgreSQL cf_raw table.

	Args:
		batch_size: Size of each batch for write operations
		num_processes: Number of parallel processes (defaults to min(CPU count, 8))
		tax_codes: List of tax codes to write

	Returns:
		float: Total elapsed time in seconds, or None on error
	"""
	if num_processes is None:
		num_processes = min(mp.cpu_count(), 8)

	if not tax_codes:
		print("❌ No tax codes provided for cf_raw write")
		return None

	print("\n" + "=" * 60)
	print("📥 BENCHMARK 1bis: WRITE TAX CODES TO POSTGRESQL (cf_raw)")
	print("=" * 60)
	print(f"   Records to insert: {len(tax_codes):,}")
	print(f"   Parallel processes: {num_processes}")
	print(f"   Batch size: {batch_size:,}")

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

	# PHASE 1: Truncate table for fresh insert
	print("   - Truncating cf_raw table...")
	try:
		cur_admin.execute("TRUNCATE TABLE cf_raw")
		conn_admin.commit()
		print("   ✓ Table truncated")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	print("   - Configuring optimal parameters...")
	cur_admin.execute("SET maintenance_work_mem = '2GB'")
	print("   ✓ Parameters configured")
	print("")

	# PHASE 2: Bulk insertion
	print("📥 INSERT PHASE: COPY FROM STDIN...")
	start_time = time.time()

	# Split data into batches
	batch_data = []
	for i in range(0, len(tax_codes), batch_size):
		batch_tax_codes = tax_codes[i:i+batch_size]
		batch_data.append((batch_tax_codes, i // batch_size))

	chunksize = max(1, len(batch_data) // (num_processes * 8))
	with mp.Pool(processes=num_processes) as pool:
		results = pool.map(write_cf_raw_batch_worker, batch_data, chunksize=chunksize)

	insert_time = time.time() - start_time
	total_count = sum(results)

	print(f"✅ Insert completed: {total_count:,} tax codes in {insert_time:.2f}s")
	print(f"   Throughput: {total_count / insert_time:,.0f} tax codes/sec")
	print("")

	# PHASE 3: ANALYZE
	print("   - Executing ANALYZE...")
	analyze_start = time.time()
	cur_admin.execute("ANALYZE cf_raw")
	conn_admin.commit()
	analyze_time = time.time() - analyze_start
	print(f"   ✓ ANALYZE completed in {analyze_time:.2f}s")

	cur_admin.close()
	conn_admin.close()

	total_time = time.time() - start_time

	print("")
	print("📊 SUMMARY:")
	print(f"   - Data insertion: {insert_time:.2f}s ({total_count / insert_time:,.0f} tax codes/sec)")
	print(f"   - ANALYZE: {analyze_time:.2f}s")
	print(f"   - TOTAL: {total_time:.2f}s")

	return total_time


def get_tax_codes_in_batches(source, mega_batch_size: int = 2_000_000):
	"""
	Universal generator that yields mega-batches of tax codes from either DB or memory.

	Args:
		source: Either 'db' to read from PostgreSQL cf_raw, or a list of tax codes
		mega_batch_size: Number of records to yield per mega-batch (default: 2M)

	Yields:
		List[str]: Mega-batch of tax codes (up to mega_batch_size records)
	"""
	if source == 'db':
		# Read from PostgreSQL cf_raw table
		yield from read_tax_codes_from_cf_raw_in_batches(mega_batch_size=mega_batch_size)
	else:
		# Chunk in-memory list into mega-batches
		tax_codes_list = source
		total_records = len(tax_codes_list)

		for i in range(0, total_records, mega_batch_size):
			mega_batch = tax_codes_list[i:i + mega_batch_size]
			batch_num = (i // mega_batch_size) + 1
			total_batches = (total_records + mega_batch_size - 1) // mega_batch_size

			print(f"   📦 Mega-batch {batch_num}/{total_batches}: {len(mega_batch):,} records")
			yield mega_batch


def read_tax_codes_from_cf_raw_in_batches(mega_batch_size: int = 2_000_000, fetch_batch_size: int = 500_000):
	"""
	Generator that yields mega-batches of tax codes from PostgreSQL cf_raw table.

	This function uses a server-side cursor to fetch data in chunks, yielding
	mega-batches to avoid loading all 65M+ records into memory at once.

	Args:
		mega_batch_size: Number of records to yield per mega-batch (default: 2M)
		fetch_batch_size: Number of records to fetch from DB at a time (default: 500k)

	Yields:
		List[str]: Mega-batch of tax codes (up to mega_batch_size records)
	"""
	print(f"📥 Reading tax codes from PostgreSQL cf_raw table in mega-batches ({mega_batch_size:,} per batch)...")

	conn = psycopg2.connect(
		host=POSTGRES_HOST,
		port=POSTGRES_PORT,
		database=POSTGRES_DB,
		user=POSTGRES_USER,
		password=POSTGRES_PASSWORD
	)

	# Use named cursor for server-side cursor (memory efficient)
	cur = conn.cursor(name='fetch_cf_raw')
	cur.itersize = fetch_batch_size  # Fetch fetch_batch_size rows at a time

	try:
		start_time = time.time()

		# Read tax codes in batches using server-side cursor
		cur.execute("SELECT codice_fiscale FROM cf_raw ORDER BY codice_fiscale")

		mega_batch_buffer = []
		total_read = 0
		mega_batch_count = 0

		while True:
			rows = cur.fetchmany(fetch_batch_size)
			if not rows:
				break

			# Add fetched rows to buffer
			mega_batch_buffer.extend([row[0] for row in rows])
			total_read += len(rows)

			# When buffer reaches mega_batch_size, yield it
			while len(mega_batch_buffer) >= mega_batch_size:
				mega_batch_count += 1
				yield_batch = mega_batch_buffer[:mega_batch_size]
				mega_batch_buffer = mega_batch_buffer[mega_batch_size:]

				print(f"   📦 Mega-batch {mega_batch_count}: {len(yield_batch):,} records (total read: {total_read:,})")
				yield yield_batch

		# Yield remaining records
		if mega_batch_buffer:
			mega_batch_count += 1
			print(f"   📦 Mega-batch {mega_batch_count} (final): {len(mega_batch_buffer):,} records (total read: {total_read:,})")
			yield mega_batch_buffer

		elapsed = time.time() - start_time

		print(f"✅ Read complete: {total_read:,} tax codes in {elapsed:.2f}s ({mega_batch_count} mega-batches)")
		print(f"   Throughput: {total_read / elapsed:,.0f} records/sec")
		print("")

	finally:
		cur.close()
		conn.close()


def write_keydb_batch_worker(args):
	"""
	Worker function for KeyDB batch write operations (multiprocessing).

	Optimized: uses MSET for bulk writes (faster than individual SETs)
	Computes hashes from tax codes

	Args:
		args: Tuple of (tax_codes, worker_id)
		      tax_codes: List of tax code strings

	Returns:
		int: Number of records written
	"""
	tax_codes, worker_id = args

	# Use connection pool to reuse connections across workers
	r = redis.Redis(connection_pool=get_redis_pool())

	# Compute all hashes first
	mapping = {}
	for tax_code in tax_codes:
		hash_val = compute_sha256_hash(tax_code)
		# KeyDB stores: hash -> tax_code
		mapping[hash_val] = tax_code

	# Use MSET for bulk write (single command, much faster than pipeline with multiple SETs)
	# MSET atomically sets all key-value pairs in one operation
	r.mset(mapping)

	return len(tax_codes)


def benchmark_keydb(batch_size: int = 10_000, num_processes: int = None, tax_codes: List[str] = None, incremental_mode: bool = False) -> Optional[float]:
	"""
	BENCHMARK 2: KeyDB write operations - computes hashes and writes to KeyDB.

	Optimized with Pipeline + hash computation.

	Args:
		batch_size: Size of each batch for write operations
		num_processes: Number of parallel processes (defaults to CPU count)
		tax_codes: List of tax codes (hashes will be computed)
		incremental_mode: If True, skips header/footer (for mega-batch processing)

	Returns:
		float: Total elapsed time in seconds, or None on error
	"""
	if num_processes is None:
		num_processes = mp.cpu_count()

	if not tax_codes:
		if not incremental_mode:
			print("❌ No tax codes provided for KeyDB benchmark")
		return None

	if not incremental_mode:
		print("\n" + "=" * 60)
		print("📮 BENCHMARK 2: HASH COMPUTATION + KEYDB WRITE")
		print("=" * 60)
		print(f"   Tax codes to process: {len(tax_codes):,}")
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
		print("📥 HASH + INSERT PHASE: Computing hashes and writing to KeyDB...")
	else:
		# In incremental mode, just verify connection
		try:
			r = redis.Redis(host=KEYDB_HOST, port=KEYDB_PORT, db=KEYDB_DB, decode_responses=False)
			r.ping()
		except Exception as e:
			print(f"❌ KeyDB connection error: {e}")
			return None

	start_time = time.time()

	# Split data into batches
	batch_data = []
	for i in range(0, len(tax_codes), batch_size):
		batch_tax_codes = tax_codes[i:i+batch_size]
		batch_data.append((batch_tax_codes, i // batch_size))

	chunksize = max(1, len(batch_data) // (num_processes * 4))
	with mp.Pool(processes=num_processes) as pool:
		results = pool.map(write_keydb_batch_worker, batch_data, chunksize=chunksize)

	insert_time = time.time() - start_time
	total_count = sum(results)

	if not incremental_mode:
		print(f"✅ Completed: {total_count:,} records in {insert_time:.2f}s")
		print(f"   Throughput: {total_count / insert_time:,.0f} records/sec")
	else:
		print(f"   ✓ Processed {total_count:,} records in {insert_time:.2f}s ({total_count / insert_time:,.0f} rec/s)")

	return insert_time


def write_postgres_batch_worker(args):
	"""
	Worker function for PostgreSQL batch write operations (multiprocessing).

	Optimized: uses COPY FROM STDIN (2-5x faster than INSERT)
	Computes hashes from tax codes

	Args:
		args: Tuple of (tax_codes, worker_id)
		      tax_codes: List of tax code strings

	Returns:
		int: Number of records written
	"""
	tax_codes, worker_id = args

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

		# Compute hash for each tax code
		# Note: Write as (hash, tax_code) to match table structure (hash is PRIMARY KEY)
		for tax_code in tax_codes:
			hash_val = compute_sha256_hash(tax_code)
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

		return len(tax_codes)
	finally:
		cur.close()
		conn.close()


def postgres_insert_init():
	"""
	Initialize PostgreSQL for bulk insert: check table type, drop indexes, set parameters.

	Returns:
		Tuple[cursor, connection]: Admin cursor and connection to use for subsequent operations
	"""
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
		return None, None

	print("")
	print("📋 PRE-INSERT PHASE: Table preparation...")

	# PHASE 1: Drop existing indexes and PRIMARY KEY
	print("   - Dropping existing indexes and PRIMARY KEY...")
	try:
		cur_admin.execute("DROP INDEX IF EXISTS idx_codici_fiscali_cf")
		cur_admin.execute("ALTER TABLE codici_fiscali DROP CONSTRAINT IF EXISTS codici_fiscali_codice_fiscale_key")
		cur_admin.execute("ALTER TABLE codici_fiscali DROP CONSTRAINT IF EXISTS codici_fiscali_pkey")
		conn_admin.commit()
		print("   ✓ Indexes and PRIMARY KEY dropped")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	# PHASE 2: TRUNCATE table
	print("   - Truncating table...")
	try:
		cur_admin.execute("TRUNCATE TABLE codici_fiscali")
		conn_admin.commit()
		print("   ✓ Table truncated")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	# PHASE 3: Disable autovacuum on codici_fiscali table
	print("   - Disabling autovacuum on codici_fiscali...")
	try:
		cur_admin.execute("ALTER TABLE codici_fiscali SET (autovacuum_enabled = false)")
		conn_admin.commit()
		print("   ✓ Autovacuum disabled")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	# PHASE 4: Set optimal parameters
	print("   - Configuring optimal parameters...")
	cur_admin.execute("SET maintenance_work_mem = '2GB'")
	print("   ✓ Parameters configured")
	print("")

	return cur_admin, conn_admin


def postgres_insert_finalize(cur_admin, conn_admin) -> float:
	"""
	Finalize PostgreSQL bulk insert: recreate indexes and run ANALYZE.

	Args:
		cur_admin: Admin cursor
		conn_admin: Admin connection

	Returns:
		float: Time spent on finalization (indexes + ANALYZE)
	"""
	finalize_start = time.time()

	# PHASE 1: Recreate PRIMARY KEY
	print("🔨 POST-INSERT PHASE: Index creation...")
	index_start_time = time.time()

	try:
		print("   - Creating PRIMARY KEY on codice_fiscale...")
		cur_admin.execute("ALTER TABLE codici_fiscali ADD PRIMARY KEY (codice_fiscale)")
		conn_admin.commit()

		index_time = time.time() - index_start_time
		print(f"   ✓ PRIMARY KEY created in {index_time:.2f}s")
	except Exception as e:
		print(f"   ✗ PRIMARY KEY creation error: {e}")
		conn_admin.rollback()
		index_time = 0

	# PHASE 2: Re-enable autovacuum on codici_fiscali table
	print("   - Re-enabling autovacuum on codici_fiscali...")
	try:
		cur_admin.execute("ALTER TABLE codici_fiscali SET (autovacuum_enabled = true)")
		conn_admin.commit()
		print("   ✓ Autovacuum re-enabled")
	except Exception as e:
		print(f"   ⚠️  Warning: {e}")
		conn_admin.rollback()

	# PHASE 3: ANALYZE
	print("   - Executing ANALYZE...")
	analyze_start_time = time.time()
	cur_admin.execute("ANALYZE codici_fiscali")
	conn_admin.commit()
	analyze_time = time.time() - analyze_start_time
	print(f"   ✓ ANALYZE completed in {analyze_time:.2f}s")

	cur_admin.close()
	conn_admin.close()

	return time.time() - finalize_start


def benchmark_postgres_insert(batch_size: int = 50_000, num_processes: int = None, tax_codes: List[str] = None, incremental_mode: bool = False, mega_batch_num: int = None) -> Optional[tuple]:
	"""
	BENCHMARK 3: PostgreSQL write operations - computes hashes and writes to PostgreSQL.

	Optimized with COPY FROM STDIN + index management + hash computation.

	Args:
		batch_size: Size of each batch for write operations
		num_processes: Number of parallel processes (defaults to min(CPU count, 8))
		tax_codes: List of tax codes (hashes will be computed)
		incremental_mode: If True, only performs insert (no init/finalize)
		mega_batch_num: Mega-batch number for logging (when incremental_mode=True)

	Returns:
		tuple: (total_time, hash_time, insert_time) or None on error
	"""
	if num_processes is None:
		num_processes = min(mp.cpu_count(), 8)

	if not tax_codes:
		if not incremental_mode:
			print("❌ No tax codes provided for PostgreSQL benchmark")
		return None

	if not incremental_mode:
		print("\n" + "=" * 60)
		print("🗄️  BENCHMARK 3: HASH COMPUTATION + POSTGRESQL WRITE")
		print("=" * 60)
		print(f"   Tax codes to process: {len(tax_codes):,}")
		print(f"   Parallel processes: {num_processes}")
		print(f"   Batch size: {batch_size:,}")

	# Bulk insertion phase
	if not incremental_mode:
		print("📥 HASH + INSERT PHASE: Computing hashes and writing to PostgreSQL...")

	start_time = time.time()

	# PHASE 1: Compute hashes
	hash_start = time.time()
	chunk_size = max(1, len(tax_codes) // num_processes)
	chunks = [tax_codes[i:i + chunk_size] for i in range(0, len(tax_codes), chunk_size)]

	with mp.Pool(processes=num_processes) as pool:
		hash_results = pool.map(compute_hash_batch, chunks)

	hashes = []
	for result in hash_results:
		hashes.extend(result)

	hash_time = time.time() - hash_start

	if incremental_mode and mega_batch_num:
		print(f"   🔢 Mega-batch {mega_batch_num}: Hashed {len(hashes):,} records in {hash_time:.2f}s ({len(hashes) / hash_time:,.0f} hash/s)")

	# PHASE 2: Insert data
	insert_start = time.time()
	data_to_insert = list(zip(hashes, tax_codes))

	# Split into batches for parallel COPY
	batch_data = []
	for i in range(0, len(data_to_insert), batch_size):
		batch = data_to_insert[i:i+batch_size]
		batch_data.append((batch, i // batch_size))

	chunksize = max(1, len(batch_data) // (num_processes * 8))
	with mp.Pool(processes=num_processes) as pool:
		results = pool.map(write_salt_update_batch_worker, batch_data, chunksize=chunksize)

	insert_time = time.time() - insert_start
	total_count = sum(results)

	total_time = time.time() - start_time

	if not incremental_mode:
		print(f"✅ Insert completed: {total_count:,} tax codes in {total_time:.2f}s")
		print(f"   Throughput: {total_count / total_time:,.0f} tax codes/sec")
	elif mega_batch_num:
		print(f"   💾 Mega-batch {mega_batch_num}: Inserted {total_count:,} records in {insert_time:.2f}s ({total_count / insert_time:,.0f} ins/s)")

	return (total_time, hash_time, insert_time)


def read_from_codici_fiscali_in_batches(mega_batch_size: int = 2_000_000, fetch_batch_size: int = 500_000):
	"""
	Generator that yields mega-batches of tax codes from PostgreSQL codici_fiscali table.

	Reads existing tax codes for salt rotation scenario.

	Args:
		mega_batch_size: Number of records to yield per mega-batch (default: 2M)
		fetch_batch_size: Number of records to fetch from DB at a time (default: 500k)

	Yields:
		List[str]: Mega-batch of tax codes (up to mega_batch_size records)
	"""
	print(f"📥 Reading tax codes from PostgreSQL codici_fiscali table in mega-batches ({mega_batch_size:,} per batch)...")

	conn = psycopg2.connect(
		host=POSTGRES_HOST,
		port=POSTGRES_PORT,
		database=POSTGRES_DB,
		user=POSTGRES_USER,
		password=POSTGRES_PASSWORD
	)

	# Use named cursor for server-side cursor
	cur = conn.cursor(name='fetch_codici_fiscali_salt')
	cur.itersize = fetch_batch_size

	try:
		start_time = time.time()

		# Read only codice_fiscale (we'll recalculate hash with NEW_SALT)
		cur.execute("SELECT codice_fiscale FROM codici_fiscali ORDER BY codice_fiscale")

		mega_batch_buffer = []
		total_read = 0
		mega_batch_count = 0

		while True:
			rows = cur.fetchmany(fetch_batch_size)
			if not rows:
				break

			# Add fetched rows to buffer
			mega_batch_buffer.extend([row[0] for row in rows])
			total_read += len(rows)

			# When buffer reaches mega_batch_size, yield it
			while len(mega_batch_buffer) >= mega_batch_size:
				mega_batch_count += 1
				yield_batch = mega_batch_buffer[:mega_batch_size]
				mega_batch_buffer = mega_batch_buffer[mega_batch_size:]

				print(f"   📦 Mega-batch {mega_batch_count}: {len(yield_batch):,} records (total read: {total_read:,})")
				yield yield_batch

		# Yield remaining records
		if mega_batch_buffer:
			mega_batch_count += 1
			print(f"   📦 Mega-batch {mega_batch_count} (final): {len(mega_batch_buffer):,} records (total read: {total_read:,})")
			yield mega_batch_buffer

		elapsed = time.time() - start_time

		print(f"✅ Read complete: {total_read:,} tax codes in {elapsed:.2f}s ({mega_batch_count} mega-batches)")
		print(f"   Throughput: {total_read / elapsed:,.0f} records/sec")
		print("")

	finally:
		cur.close()
		conn.close()


def benchmark_postgres_salt_update(batch_size: int = 50_000, num_processes: int = None, mega_batch_size: int = 2_000_000) -> Optional[float]:
	"""
	BENCHMARK 4: Update PostgreSQL hashes with new salt (salt rotation scenario).

	Uses mega-batch processing: reads from codici_fiscali in chunks, recalculates hashes,
	and repopulates table using TRUNCATE + COPY strategy.

	Args:
		batch_size: Size of each batch for COPY operations
		num_processes: Number of parallel processes for hash computation
		mega_batch_size: Size of mega-batches for reading/processing

	Returns:
		float: Total elapsed time in seconds, or None on error
	"""
	print("\n" + "=" * 60)
	print("🔄 BENCHMARK 4: SALT ROTATION (MEGA-BATCH PROCESSING)")
	print("=" * 60)
	print(f"   Old salt: {SALT}")
	print(f"   New salt: {NEW_SALT}")
	print(f"   Mega-batch size: {mega_batch_size:,}")
	print(f"   COPY batch size: {batch_size:,}")
	print("")

	# Determine number of processes
	if num_processes is None:
		num_processes = max(1, mp.cpu_count() - 1)

	start_time = time.time()

	# Initialize: drop constraint, truncate table
	print("🔧 INITIALIZATION PHASE...")
	cur_admin, conn_admin = postgres_insert_init()
	if not cur_admin:
		print("❌ Failed to initialize PostgreSQL")
		return None

	print("")

	# MEGA-BATCH PROCESSING: Read → Hash → Insert
	print(f"🔄 PROCESSING PHASE: Reading from cf_raw in mega-batches of {mega_batch_size:,}...")
	total_read = 0
	total_hashed = 0
	total_inserted = 0
	mega_batch_num = 0
	total_hash_time = 0.0
	total_insert_time = 0.0

	for mega_batch_tax_codes in read_tax_codes_from_cf_raw_in_batches(mega_batch_size=mega_batch_size):
		mega_batch_num += 1
		total_read += len(mega_batch_tax_codes)

		# Recalculate hashes with NEW_SALT for this mega-batch
		hash_start = time.time()
		chunk_size = max(1, len(mega_batch_tax_codes) // num_processes)
		chunks = [mega_batch_tax_codes[i:i + chunk_size] for i in range(0, len(mega_batch_tax_codes), chunk_size)]

		with mp.Pool(processes=num_processes) as pool:
			results = pool.map(compute_hash_batch_with_new_salt, chunks)

		new_hashes = []
		for result in results:
			new_hashes.extend(result)

		hash_time = time.time() - hash_start
		total_hashed += len(new_hashes)
		total_hash_time += hash_time

		print(f"   🔢 Mega-batch {mega_batch_num}: Hashed {len(new_hashes):,} records in {hash_time:.2f}s ({len(new_hashes) / hash_time:,.0f} hash/s)")

		# Insert this mega-batch with new hashes
		insert_start = time.time()
		data_to_insert = list(zip(new_hashes, mega_batch_tax_codes))

		# Split into batches for parallel COPY
		batch_data = []
		for i in range(0, len(data_to_insert), batch_size):
			batch = data_to_insert[i:i+batch_size]
			batch_data.append((batch, i // batch_size))

		num_copy_processes = min(mp.cpu_count(), 8)
		chunksize = max(1, len(batch_data) // (num_copy_processes * 8))

		with mp.Pool(processes=num_copy_processes) as pool:
			results = pool.map(write_salt_update_batch_worker, batch_data, chunksize=chunksize)

		insert_time = time.time() - insert_start
		inserted_count = sum(results)
		total_inserted += inserted_count
		total_insert_time += insert_time

		print(f"   💾 Mega-batch {mega_batch_num}: Inserted {inserted_count:,} records in {insert_time:.2f}s ({inserted_count / insert_time:,.0f} ins/s)")
		print("")

	print(f"✅ All mega-batches processed: {mega_batch_num} batches, {total_read:,} total records")
	print("")

	# Check if cf_raw was empty
	if total_read == 0:
		print("⚠️  No records found in cf_raw table")
		print("   BENCHMARK 4 requires data in cf_raw.")
		print("   Run BENCHMARK 1bis first to populate cf_raw.")
		cur_admin.close()
		conn_admin.close()
		return None

	# Finalize: recreate constraint, ANALYZE
	print("🔨 FINALIZATION PHASE...")
	finalize_time = postgres_insert_finalize(cur_admin, conn_admin)

	total_time = time.time() - start_time

	print("")
	print("📊 SUMMARY:")
	print(f"   - Total records processed: {total_read:,}")

	# Avoid division by zero
	hash_rate = (total_hashed / total_hash_time) if total_hash_time > 0 else 0
	insert_rate = (total_inserted / total_insert_time) if total_insert_time > 0 else 0

	print(f"   - Hash recalculation: {total_hash_time:.2f}s ({hash_rate:,.0f} hashes/sec)")
	print(f"   - Database repopulation: {total_insert_time:.2f}s ({insert_rate:,.0f} inserts/sec)")
	print(f"   - Finalization: {finalize_time:.2f}s")
	print(f"   - TOTAL: {total_time:.2f}s")
	print("")

	return total_time


def compute_hash_batch(tax_codes: List[str]) -> List[str]:
	"""
	Worker function for computing hashes with SALT (for BENCHMARK 3).

	Args:
		tax_codes: List of tax codes to hash

	Returns:
		List of SHA256 hashes computed with SALT
	"""
	return [compute_sha256_hash(cf) for cf in tax_codes]


def compute_hash_batch_with_new_salt(tax_codes: List[str]) -> List[str]:
	"""
	Worker function for computing hashes with NEW_SALT (for BENCHMARK 4).

	Args:
		tax_codes: List of tax codes to hash

	Returns:
		List of SHA256 hashes computed with NEW_SALT
	"""
	return [hashlib.sha256(cf.encode('utf-8') + NEW_SALT_BYTES).hexdigest() for cf in tax_codes]


def write_salt_update_batch_worker(args):
	"""
	Worker function for writing (new_hash, codice_fiscale) pairs to PostgreSQL (BENCHMARK 4).

	Uses COPY FROM STDIN for maximum performance during salt rotation.

	Args:
		args: Tuple of (data_batch, worker_id)
		      data_batch: List of (hash, codice_fiscale) tuples

	Returns:
		int: Number of records written
	"""
	data_batch, worker_id = args

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

		# Write data as (hash, codice_fiscale) - hash is PRIMARY KEY
		for hash_val, tax_code in data_batch:
			buffer.write(f"{hash_val},{tax_code}\n")

		buffer.seek(0)

		# COPY FROM STDIN is 10-20x faster than INSERT/UPDATE
		try:
			cur.copy_expert(
				"COPY codici_fiscali (hash, codice_fiscale) FROM STDIN WITH (FORMAT CSV)",
				buffer
			)
		except psycopg2.errors.UniqueViolation:
			# Ignore duplicates (should not occur with unique hashes)
			conn.rollback()
		else:
			conn.commit()

		return len(data_batch)
	finally:
		cur.close()
		conn.close()


def main():
	"""Main entry point for benchmark execution."""
	# Benchmark parameters (configurable via environment variables)
	TOTAL_IDS = int(os.getenv('TOTAL_IDS', '6000000'))
	BATCH_SIZE_COMPUTATION = int(os.getenv('BATCH_SIZE_COMPUTATION', '100000'))
	BATCH_SIZE_CF_RAW = int(os.getenv('BATCH_SIZE_CF_RAW', '50000'))
	BATCH_SIZE_KEYDB = int(os.getenv('BATCH_SIZE_KEYDB', '10000'))
	BATCH_SIZE_POSTGRES = int(os.getenv('BATCH_SIZE_POSTGRES', '50000'))
	BATCH_SIZE_POSTGRES_UPDATE = int(os.getenv('BATCH_SIZE_POSTGRES_UPDATE', '50000'))

	# Flags to enable/disable individual benchmarks (configurable via environment variables)
	RUN_POSTGRES_CF_WRITE = os.getenv('RUN_POSTGRES_CF_WRITE', 'true').lower() == 'true'
	RUN_KEYDB = os.getenv('RUN_KEYDB', 'true').lower() == 'true'
	RUN_POSTGRES_INSERT = os.getenv('RUN_POSTGRES_INSERT', 'true').lower() == 'true'
	RUN_POSTGRES_SALT_UPDATE = os.getenv('RUN_POSTGRES_SALT_UPDATE', 'true').lower() == 'true'

	print("\n" + "=" * 60)
	print("🚀 BENCHMARK: TAX CODE -> SHA256 HASH")
	print("=" * 60)
	print(f"Total records: {TOTAL_IDS:,}")
	print(f"Available CPUs: {mp.cpu_count()}")

	# BENCHMARK 1: Generate tax codes (no hashes) - using NumPy for speed
	time_generation, all_tax_codes = generate_tax_codes_only_numpy(TOTAL_IDS)

	# BENCHMARK 1bis: Write tax codes to PostgreSQL cf_raw table (if enabled)
	time_cf_raw_write = None
	if RUN_POSTGRES_CF_WRITE:
		time_cf_raw_write = benchmark_postgres_cf_raw_write(BATCH_SIZE_CF_RAW, tax_codes=all_tax_codes)

	# Determine source for subsequent benchmarks
	# If we wrote to cf_raw, use 'db'; otherwise use in-memory list
	MEGA_BATCH_SIZE = int(os.getenv('MEGA_BATCH_SIZE', '2000000'))  # 2M records per batch
	source = 'db' if RUN_POSTGRES_CF_WRITE else all_tax_codes

	# BENCHMARK 2: Compute hashes + write to KeyDB (if enabled)
	# Each benchmark reads independently from source in mega-batches
	time_keydb = None
	if RUN_KEYDB:
		print("\n" + "=" * 60)
		print("📮 BENCHMARK 2: HASH COMPUTATION + KEYDB WRITE")
		print("=" * 60)
		print(f"   Source: {'PostgreSQL cf_raw' if source == 'db' else 'In-memory'}")
		print(f"   Mega-batch size: {MEGA_BATCH_SIZE:,}")
		print(f"   Batch size: {BATCH_SIZE_KEYDB:,}")
		print("")

		time_keydb = 0.0
		mega_batch_num = 0

		for mega_batch in get_tax_codes_in_batches(source, mega_batch_size=MEGA_BATCH_SIZE):
			mega_batch_num += 1
			batch_time = benchmark_keydb(BATCH_SIZE_KEYDB, tax_codes=mega_batch, incremental_mode=True)
			if batch_time:
				time_keydb += batch_time

		print(f"\n✅ BENCHMARK 2 complete: {mega_batch_num} mega-batches processed")
		print(f"   Total time: {time_keydb:.2f}s")

	# BENCHMARK 3: Compute hashes + write to PostgreSQL codici_fiscali (if enabled)
	# Reads independently from source in mega-batches
	time_postgres = None
	time_postgres_total = None
	if RUN_POSTGRES_INSERT:
		print("\n" + "=" * 60)
		print("🗄️  BENCHMARK 3: HASH COMPUTATION + POSTGRESQL WRITE")
		print("=" * 60)
		print(f"   Source: {'PostgreSQL cf_raw' if source == 'db' else 'In-memory'}")
		print(f"   Mega-batch size: {MEGA_BATCH_SIZE:,}")
		print(f"   Batch size: {BATCH_SIZE_POSTGRES:,}")
		print("")

		# Initialize: drop indexes, truncate, configure
		cur_admin, conn_admin = postgres_insert_init()
		if not cur_admin:
			print("❌ Failed to initialize PostgreSQL")
			time_postgres = None
		else:
			time_postgres = 0.0
			total_hash_time = 0.0
			total_insert_time = 0.0
			mega_batch_num = 0

			print("📥 HASH + INSERT PHASE: Computing hashes and writing to PostgreSQL...")
			for mega_batch in get_tax_codes_in_batches(source, mega_batch_size=MEGA_BATCH_SIZE):
				mega_batch_num += 1
				result = benchmark_postgres_insert(BATCH_SIZE_POSTGRES, tax_codes=mega_batch, incremental_mode=True, mega_batch_num=mega_batch_num)
				if result:
					batch_total, batch_hash, batch_insert = result
					time_postgres += batch_total
					total_hash_time += batch_hash
					total_insert_time += batch_insert

			# Finalize: recreate indexes, ANALYZE
			finalize_time = postgres_insert_finalize(cur_admin, conn_admin)

			# Calculate total time including finalization
			time_postgres_total = time_postgres + finalize_time

			print(f"\n✅ BENCHMARK 3 complete: {mega_batch_num} mega-batches processed")
			print(f"   Hash computation: {total_hash_time:.2f}s ({TOTAL_IDS / total_hash_time:,.0f} hash/s)")
			print(f"   Database insertion: {total_insert_time:.2f}s ({TOTAL_IDS / total_insert_time:,.0f} ins/s)")
			print(f"   Finalization: {finalize_time:.2f}s")
			print(f"   Total time: {time_postgres_total:.2f}s")

	# BENCHMARK 4: Salt update + hash recalculation on PostgreSQL (if enabled)
	# Uses mega-batch processing to handle 65M+ records efficiently
	time_salt_update = None
	if RUN_POSTGRES_SALT_UPDATE:
		time_salt_update = benchmark_postgres_salt_update(
			batch_size=BATCH_SIZE_POSTGRES_UPDATE,
			mega_batch_size=MEGA_BATCH_SIZE
		)

	# Final summary
	print("\n" + "=" * 60)
	print("📊 RESULTS SUMMARY")
	print("=" * 60)
	print(f"1️⃣    Tax code generation:         {time_generation:8.2f}s  ({TOTAL_IDS/time_generation:,.0f} op/s)")

	if time_cf_raw_write:
		print(f"1️⃣ bis CF write to PostgreSQL:     {time_cf_raw_write:8.2f}s  ({TOTAL_IDS/time_cf_raw_write:,.0f} op/s)")

	if time_keydb:
		print(f"2️⃣    Hash + KeyDB write:          {time_keydb:8.2f}s  ({TOTAL_IDS/time_keydb:,.0f} op/s)")

	if time_postgres_total:
		print(f"3️⃣    Hash + PostgreSQL write:     {time_postgres_total:8.2f}s  ({TOTAL_IDS/time_postgres_total:,.0f} op/s)")

	if time_salt_update:
		print(f"4️⃣    Salt update (hash recalc):   {time_salt_update:8.2f}s  ({TOTAL_IDS/time_salt_update:,.0f} op/s)")

	print("=" * 60)


if __name__ == "__main__":
	main()
