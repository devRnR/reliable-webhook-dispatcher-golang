package store

import (
	"context"
	"database/sql"
	"encoding/json"
)

type OrderStore struct {
	DB *sql.DB
}

func NewOrderStore(db *sql.DB) *OrderStore {
	return &OrderStore{DB: db}
}

type CreateOrderInput struct {
	CustomerID string
	Amount     string
}

type CreateOrderResult struct {
	OrderID string
	EventID string
	Status  string
}

func (s *OrderStore) CreateOrderWithOutbox(ctx context.Context, in CreateOrderInput) (CreateOrderResult, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return CreateOrderResult{}, err
	}

	defer tx.Rollback()

	var orderID string

	// todo QueryRowContext vs QueryRow

	if err := tx.QueryRowContext(ctx,
		`INSERT INTO orders (customer_id, amount) VALUES ($1, $2) RETURNING id`,
		in.CustomerID, in.Amount,
	).Scan(&orderID); err != nil {
		return CreateOrderResult{}, err
	}

	payload, err := json.Marshal(map[string]string{
		"event_type":  "order.created",
		"order_id":    orderID,
		"customer_id": in.CustomerID,
		"amount":      in.Amount,
	})
	if err != nil {
		return CreateOrderResult{}, err
	}

	var eventID string
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status)
		 VALUES ('order', $1, 'order.created', $2, $3) RETURNING id`,
		orderID, payload, OutboxStatusPending,
	).Scan(&eventID); err != nil {
		return CreateOrderResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return CreateOrderResult{}, err
	}

	return CreateOrderResult{OrderID: orderID, EventID: eventID, Status: "CREATED"}, nil
}
