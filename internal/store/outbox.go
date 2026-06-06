package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	OutboxStatusPending    = "PENDING"
	OutboxStatusProcessing = "PROCESSING"
	OutboxStatusSent       = "SENT"
	OutboxStatusFailed     = "FAILED"
)

type OutboxEvent struct {
	ID                  string     `json:"id"`
	AggregateType       string     `json:"aggregate_type"`
	AggregateID         string     `json:"aggregate_id"`
	EventType           string     `json:"event_type"`
	Status              string     `json:"status"`
	AttemptCount        int        `json:"attempt_count"`
	NextRetryAt         *time.Time `json:"next_retry_at"`
	ClaimToken          *string    `json:"claim_token"`
	ProcessingStartedAt *time.Time `json:"processing_started_at"`
	CreatedAt           *time.Time `json:"created_at"`
	UpdatedAt           *time.Time `json:"updated_at"`
}

type OutboxStore struct {
	DB *sql.DB
}

func NewOutboxStore(db *sql.DB) *OutboxStore {
	return &OutboxStore{DB: db}
}

func (s *OutboxStore) List(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, aggregate_type, aggregate_id, event_type, status,
		        	  attempt_count, next_retry_at, claim_token, processing_started_at,
		              created_at, updated_at
		  		FROM outbox_events
		  		ORDER BY created_at DESC
		  		LIMIT $1`, limit)

	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()

		if err != nil {
			if rerr := rows.Err(); rerr != nil {
				err = rerr
			}
		}
	}(rows)

	events := make([]OutboxEvent, 0)
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(
			&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType, &e.Status,
			&e.AttemptCount, &e.NextRetryAt, &e.ClaimToken, &e.ProcessingStartedAt,
			&e.CreatedAt, &e.UpdatedAt,
		); err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	return events, rows.Err()
}

type ClaimedEvent struct {
	ID           string
	EventType    string
	Payload      []byte
	ClaimToken   string
	AttemptCount int
}

func (s *OutboxStore) ClaimNext(ctx context.Context) (*ClaimedEvent, error) {
	const q = `
				UPDATE outbox_events
				SET status = 'PROCESSING',
				    claim_token = gen_random_uuid(),
				    processing_started_at = now(),
				    updated_at = now()
				WHERE id = (
				    SELECT id
				    FROM outbox_events
				    WHERE status = 'PENDING'
				    AND (next_retry_at IS NULL OR next_retry_at <= now())
				    ORDER BY created_at ASC
				    LIMIT 1
				)
				RETURNING id, event_type, payload, claim_token, attempt_count`
	var e ClaimedEvent
	err := s.DB.QueryRowContext(ctx, q).Scan(&e.ID, &e.EventType, &e.Payload, &e.ClaimToken, &e.AttemptCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (s *OutboxStore) MarkSent(ctx context.Context, id, claimToken string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox_events 
			   SET status = 'SENT',
			       sent_at = now(),
			       claim_token = NULL,
			       processing_started_at = NULL,
					updated_at = now() 
			   WHERE id = $1
			     AND status = 'PROCESSING'
			     AND claim_token = $2`, id, claimToken)
	return err
}

func (s *OutboxStore) ReleaseToPending(ctx context.Context, id, claimToken string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox_events
			   SET status = 'PENDING',
			       claim_token = NULL,
			       processing_started_at = NULL,
			       updated_at = now()
			   WHERE id=$1
			   AND status = 'PROCESSING'
			   AND claim_token = $2`, id, claimToken)
	return err
}
