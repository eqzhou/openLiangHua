CREATE TABLE IF NOT EXISTS watchlist_items (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    type VARCHAR(20) NOT NULL, -- 'holding' or 'focus'
    cost NUMERIC(10, 4),
    shares INTEGER,
    note TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, ts_code, type)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_id ON watchlist_items(user_id);
