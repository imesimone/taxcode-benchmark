-- Database initialization for Italian Tax Code (Codice Fiscale) -> SHA256 benchmark

-- Configuration table for salt storage
CREATE TABLE IF NOT EXISTS config (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL
);

-- Insert the hashing salt
INSERT INTO config (key, value) VALUES ('hash_salt', 'CF_ANPR_2025_SALT_KEY')
ON CONFLICT (key) DO NOTHING;

-- Tax codes table (UNLOGGED for maximum performance)
-- UNLOGGED: does not write to WAL, 2-3x faster for bulk insert
-- WARNING: data is lost on PostgreSQL crash/restart
-- NOTE: Indexes will be created AFTER bulk insert for maximum performance
CREATE UNLOGGED TABLE IF NOT EXISTS codici_fiscali (
    hash VARCHAR(64) PRIMARY KEY,
    codice_fiscale VARCHAR(16) NOT NULL
);

-- NOTE: UNIQUE constraint on codice_fiscale is created after bulk insert
-- to avoid overhead during massive insertion operations
-- Index to be created after bulk insert:
-- - UNIQUE constraint on codice_fiscale

-- Table and column comments
COMMENT ON TABLE config IS 'Global system configuration (includes hashing salt)';
COMMENT ON TABLE codici_fiscali IS 'Master table of Italian tax codes with pre-computed hashes (Python client-side)';
COMMENT ON COLUMN codici_fiscali.hash IS 'SHA256 hash computed in Python (client-side)';
