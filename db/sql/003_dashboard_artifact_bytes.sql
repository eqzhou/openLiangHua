ALTER TABLE IF EXISTS dashboard_artifacts
ADD COLUMN IF NOT EXISTS payload_bytes BYTEA;
