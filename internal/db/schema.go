package db

const schemaSQL = `
CREATE TABLE IF NOT EXISTS raw_events (
	event_id UUID PRIMARY KEY,
	event_type TEXT NOT NULL,
	user_id TEXT NOT NULL,
	ip_address TEXT NOT NULL,
	card_hash TEXT,
	amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
	currency TEXT NOT NULL DEFAULT 'USD',
	country TEXT NOT NULL,
	merchant_id TEXT NOT NULL,
	event_timestamp TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_created_at ON raw_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw_events (user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events (event_type);

CREATE TABLE IF NOT EXISTS fraud_alerts (
	alert_id UUID PRIMARY KEY,
	event_id UUID NOT NULL,
	rule_name TEXT NOT NULL,
	user_id TEXT NOT NULL,
	severity TEXT NOT NULL,
	reason TEXT NOT NULL,
	risk_points INTEGER NOT NULL DEFAULT 0,
	metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at ON fraud_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id ON fraud_alerts (user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_rule_name ON fraud_alerts (rule_name);

CREATE TABLE IF NOT EXISTS user_risk_scores (
	user_id TEXT PRIMARY KEY,
	risk_score INTEGER NOT NULL DEFAULT 0,
	last_alert_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_risk_scores_score ON user_risk_scores (risk_score DESC);
`
