CREATE TABLE idempotency_keys
(
    key             TEXT        NOT NULL,
    endpoint        TEXT        NOT NULL,
    request_hash    TEXT        NOT NULL,
    response_status INTEGER     NOT NULL,
    response_body   JSONB       NOT NULL,
    order_id        UUID        NOT NULL REFERENCES orders (id),
    event_id        UUID        NOT NULL REFERENCES outbox_events (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (endpoint, key)
);