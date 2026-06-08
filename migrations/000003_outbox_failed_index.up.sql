CREATE INDEX IF NOT EXISTS idx_outbox_failed
    ON outbox_events (status) WHERE status = 'FAILED';