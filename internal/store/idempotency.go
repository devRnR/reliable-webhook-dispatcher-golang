package store

import (
	"context"
	"database/sql"
)

type IdempotencyRow struct {
	RequestHash    string
	ResponseStatus int
	ResponseBody   []byte
	OrderId        string
	EventId        string
}

type IdempotencyStore struct {
	DB *sql.DB
}

func NewIdempotencyStore(db *sql.DB) *IdempotencyStore {
	return &IdempotencyStore{DB: db}
}

func (s *IdempotencyStore) Insert(ctx context.Context, tx *sql.Tx, endpoint, key, reqHash string, status int, body []byte, orderId, eventId string) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys
			(key, endpoint, request_hash, response_status, response_body, order_id, event_id)
		VALUES
			($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (endpoint, key) DO NOTHING
	`, key, endpoint, reqHash, status, body, orderId, eventId)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *IdempotencyStore) Get(ctx context.Context, tx *sql.Tx, endpoint, key string) (IdempotencyRow, error) {
	var r IdempotencyRow
	err := tx.QueryRowContext(ctx,
		`SELECT request_hash, response_status, response_body, order_id, event_id
				FROM idempotency_keys 
				WHERE endpoint = $1
				AND key = $2`, endpoint, key).Scan(&r.RequestHash, &r.ResponseStatus, &r.ResponseBody, &r.OrderId, &r.EventId)
	return r, err
}
