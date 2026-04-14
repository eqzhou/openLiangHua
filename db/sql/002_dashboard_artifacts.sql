CREATE TABLE IF NOT EXISTS dashboard_artifacts (
    artifact_key TEXT PRIMARY KEY,
    data_source TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    payload_json JSONB,
    payload_text TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS dashboard_artifacts_data_source_idx
ON dashboard_artifacts (data_source);

CREATE INDEX IF NOT EXISTS dashboard_artifacts_kind_idx
ON dashboard_artifacts (artifact_kind);
