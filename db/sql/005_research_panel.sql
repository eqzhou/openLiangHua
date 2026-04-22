CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.panel_runs (
    run_id UUID PRIMARY KEY,
    data_source TEXT NOT NULL,
    status TEXT NOT NULL,
    date_min DATE,
    date_max DATE,
    row_count BIGINT NOT NULL DEFAULT 0,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    feature_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    label_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS panel_runs_source_status_idx
ON research.panel_runs (data_source, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS research.panel (
    data_source TEXT NOT NULL,
    trade_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    name TEXT,
    industry TEXT,
    index_code TEXT,
    is_current_name_st BOOLEAN,
    is_index_member BOOLEAN,
    days_since_list INTEGER,
    pct_chg DOUBLE PRECISION,
    ret_1d DOUBLE PRECISION,
    mom_5 DOUBLE PRECISION,
    mom_20 DOUBLE PRECISION,
    mom_60 DOUBLE PRECISION,
    mom_120 DOUBLE PRECISION,
    vol_20 DOUBLE PRECISION,
    close_to_ma_20 DOUBLE PRECISION,
    vol_60 DOUBLE PRECISION,
    close_to_ma_60 DOUBLE PRECISION,
    amount_20 DOUBLE PRECISION,
    downside_vol_20 DOUBLE PRECISION,
    ret_skew_20 DOUBLE PRECISION,
    drawdown_60 DOUBLE PRECISION,
    can_enter_next_day BOOLEAN,
    ret_next_1d DOUBLE PRECISION,
    label_valid_t5 BOOLEAN,
    ret_t1_t5 DOUBLE PRECISION,
    label_valid_t10 BOOLEAN,
    ret_t1_t10 DOUBLE PRECISION,
    label_valid_t20 BOOLEAN,
    ret_t1_t20 DOUBLE PRECISION,
    run_id UUID REFERENCES research.panel_runs(run_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_source, trade_date, ts_code)
);

CREATE INDEX IF NOT EXISTS research_panel_source_date_idx
ON research.panel (data_source, trade_date DESC);

CREATE INDEX IF NOT EXISTS research_panel_source_symbol_date_idx
ON research.panel (data_source, ts_code, trade_date DESC);

CREATE INDEX IF NOT EXISTS research_panel_run_idx
ON research.panel (run_id);
