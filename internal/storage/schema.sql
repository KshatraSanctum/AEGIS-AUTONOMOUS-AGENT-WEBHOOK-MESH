-- Active Webhook Events Table (Idempotency Guard)
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id VARCHAR(255) PRIMARY KEY,
    source_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_source ON webhook_events(source_id);

-- Dead Letter Queue (DLQ) Table for Replay SaaS Engine
CREATE TABLE IF NOT EXISTS webhook_dlq (
    event_id VARCHAR(255) PRIMARY KEY,
    source_id VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);