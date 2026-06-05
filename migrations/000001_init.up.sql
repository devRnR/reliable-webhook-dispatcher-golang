CREATE
EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE orders
(
    id          UUID PRIMARY KEY        DEFAULT gen_random_uuid(),
    customer_id UUID           NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    status      TEXT           NOT NULL DEFAULT 'CREATED',
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events
(
    id                    UUID PRIMARY KEY     DEFAULT gen_random_uuid(),
    aggregate_type        TEXT        NOT NULL,
    aggregate_id          UUID        NOT NULL,
    event_type            TEXT        NOT NULL,
    payload               JSONB       NOT NULL,
    status                TEXT        NOT NULL DEFAULT 'PENDING',
    attempt_count         INT         NOT NULL DEFAULT 0,
    next_retry_at         TIMESTAMPTZ,
    last_error            TEXT,
    claim_token           UUID,
    processing_started_at TIMESTAMPTZ,
    sent_at               TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (attempt_count >= 0),
    CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'FAILED'))
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (status, next_retry_at, created_at) WHERE status = 'PENDING';

CREATE INDEX idx_outbox_processing_started
    ON outbox_events (status, processing_started_at) WHERE status = 'PROCESSING';

CREATE TABLE delivery_attempts
(
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID        NOT NULL REFERENCES outbox_events (id),
    claim_token   UUID        NOT NULL,
    target_url    TEXT        NOT NULL,
    attempt_no    INT         NOT NULL,
    response_code INT,
    response_body TEXT,
    success       BOOLEAN     NOT NULL,
    error_message TEXT,
    attempted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, claim_token, attempt_no)
);

CREATE INDEX idx_delivery_attempts_event
    ON delivery_attempts (event_id, attempted_at DESC);