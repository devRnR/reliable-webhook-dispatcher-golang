package store

import (
	"context"
	"database/sql"
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
