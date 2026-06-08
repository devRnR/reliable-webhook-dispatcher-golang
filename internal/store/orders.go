package store

import (
	"context"
	"database/sql"
	"encoding/json"
)

type OrderStore struct {
	DB   *sql.DB
	idem *IdempotencyStore
}

func NewOrderStore(db *sql.DB) *OrderStore {
	return &OrderStore{DB: db, idem: NewIdempotencyStore(db)}
}

type CreateOrderInput struct {
	CustomerID     string
	Amount         string
	IdempotencyKey string
	RequestHash    string
	Endpoint       string
}

type IdemOutcome int

const (
	IdemReserved IdemOutcome = iota
	IdemExisting
	IdemConflict
)

type CreateOrderResult struct {
	OrderID      string
	EventID      string
	Status       string
	Outcome      IdemOutcome
	CachedStatus int
	CachedBody   []byte
}

func (s *OrderStore) CreateOrderWithOutbox(ctx context.Context, in CreateOrderInput) (CreateOrderResult, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return CreateOrderResult{}, err
	}

	defer tx.Rollback()

	orderID, eventID, err := insertOrderAndOutbox(ctx, tx, in.CustomerID, in.Amount)
	if err != nil {
		return CreateOrderResult{}, err
	}

	if in.IdempotencyKey == "" {
		if err := tx.Commit(); err != nil {
			return CreateOrderResult{}, err
		}
		return CreateOrderResult{OrderID: orderID, EventID: eventID, Status: "CREATED"}, nil
	}

	respBody, err := json.Marshal(map[string]string{
		"order_id": orderID,
		"event_id": eventID,
		"status":   "CREATED",
	})
	if err != nil {
		return CreateOrderResult{}, err
	}
	reserved, err := s.idem.Insert(ctx, tx, in.Endpoint, in.IdempotencyKey, in.RequestHash, 201, respBody, orderID, eventID)
	if err != nil {
		return CreateOrderResult{}, err
	}
	if reserved {
		if err := tx.Commit(); err != nil {
			return CreateOrderResult{}, err
		}
		return CreateOrderResult{OrderID: orderID, EventID: eventID, Status: "CREATED", Outcome: IdemReserved}, nil
	}

	existing, err := s.idem.Get(ctx, tx, in.Endpoint, in.IdempotencyKey)
	if err != nil {
		return CreateOrderResult{}, err
	}
	_ = tx.Rollback()
	if existing.RequestHash == in.RequestHash {
		return CreateOrderResult{Outcome: IdemExisting, CachedStatus: existing.ResponseStatus, CachedBody: existing.ResponseBody}, nil
	}

	return CreateOrderResult{Outcome: IdemConflict}, nil
}

func insertOrderAndOutbox(ctx context.Context, tx *sql.Tx, customerID, amount string) (string, string, error) {
	var orderID string
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO orders (customer_id, amount) VALUES ($1, $2) RETURNING id`,
		customerID, amount,
	).Scan(&orderID); err != nil {
		return "", "", err
	}
	payload, err := json.Marshal(map[string]string{
		"event_type":  "order.created",
		"order_id":    orderID,
		"customer_id": customerID,
		"amount":      amount,
	})
	if err != nil {
		return "", "", err
	}
	var eventID string
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status)
		 VALUES ('order', $1, 'order.created', $2, $3) RETURNING id`,
		orderID, payload, OutboxStatusPending,
	).Scan(&eventID); err != nil {
		return "", "", err
	}
	return orderID, eventID, nil
}
