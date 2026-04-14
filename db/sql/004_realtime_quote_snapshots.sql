CREATE TABLE IF NOT EXISTS realtime_quote_batches (
    trade_date DATE NOT NULL,
    snapshot_bucket TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    requested_symbol_count INTEGER NOT NULL DEFAULT 0,
    success_symbol_count INTEGER NOT NULL DEFAULT 0,
    failed_symbols JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message TEXT NOT NULL DEFAULT '',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, snapshot_bucket)
);

CREATE INDEX IF NOT EXISTS realtime_quote_batches_fetched_at_idx
ON realtime_quote_batches (fetched_at DESC);

CREATE TABLE IF NOT EXISTS realtime_quote_rows (
    trade_date DATE NOT NULL,
    snapshot_bucket TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    quote_time TIMESTAMPTZ,
    quote_source TEXT,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, snapshot_bucket, ts_code),
    CONSTRAINT realtime_quote_rows_batch_fk
        FOREIGN KEY (trade_date, snapshot_bucket)
        REFERENCES realtime_quote_batches (trade_date, snapshot_bucket)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS realtime_quote_rows_ts_code_idx
ON realtime_quote_rows (ts_code);

CREATE INDEX IF NOT EXISTS realtime_quote_rows_quote_time_idx
ON realtime_quote_rows (quote_time DESC);
