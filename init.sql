-- Database initialization for Italian Tax Code (Codice Fiscale) -> SHA256 benchmark

-- Raw tax codes table (production-safe LOGGED table)
-- This table stores ONLY the tax codes (no hashes) for BENCHMARK 1bis
-- Used as immutable source data for subsequent benchmarks
CREATE TABLE IF NOT EXISTS cf_raw (
    codice_fiscale VARCHAR(16) PRIMARY KEY
);

COMMENT ON TABLE cf_raw IS 'Raw Italian tax codes (no hashes) - immutable source data for benchmarks';
COMMENT ON COLUMN cf_raw.codice_fiscale IS 'Italian tax code (16 alphanumeric characters)';

-- Tax codes table with hashes (production-safe LOGGED table)
-- NOTE: Indexes will be created AFTER bulk insert for maximum performance
CREATE TABLE IF NOT EXISTS codici_fiscali (
    hash VARCHAR(64) PRIMARY KEY,
    codice_fiscale VARCHAR(16) NOT NULL
);

-- NOTE: UNIQUE constraint on codice_fiscale is created after bulk insert
-- to avoid overhead during massive insertion operations
-- Index to be created after bulk insert:
-- - UNIQUE constraint on codice_fiscale

-- Table and column comments
COMMENT ON TABLE codici_fiscali IS 'Master table of Italian tax codes with pre-computed hashes (Python client-side)';
COMMENT ON COLUMN codici_fiscali.hash IS 'SHA256 hash (PRIMARY KEY) computed in Python using configurable salt (CF_HASH_SALT env var)';
COMMENT ON COLUMN codici_fiscali.codice_fiscale IS 'Italian tax code (16 alphanumeric characters)';
