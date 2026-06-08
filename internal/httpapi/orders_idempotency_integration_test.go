package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"reliable-webhook-dispatcher/internal/store"
)

func TestOrderHandler_Create_sameIdempotencyKeyAndPayload_replaysFirstResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newOrderHandlerTestDB(t, ctx, "http_order_idem_existing")
	sut := &OrderHandler{Orders: store.NewOrderStore(testDB)}
	body := []byte(`{"customer_id":"00000000-0000-0000-0000-000000000001","amount":"42.00"}`)

	firstRequest := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(body))
	firstRequest.Header.Set("Content-Type", "application/json")
	firstRequest.Header.Set(HeaderIdempotencyKey, "http-order-key-1")
	firstRecorder := httptest.NewRecorder()
	sut.Create(firstRecorder, firstRequest)

	secondRequest := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(body))
	secondRequest.Header.Set("Content-Type", "application/json")
	secondRequest.Header.Set(HeaderIdempotencyKey, "http-order-key-1")
	secondRecorder := httptest.NewRecorder()
	sut.Create(secondRecorder, secondRequest)

	if firstRecorder.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", firstRecorder.Code)
	}
	if secondRecorder.Code != firstRecorder.Code {
		t.Fatalf("second status = %d, want %d", secondRecorder.Code, firstRecorder.Code)
	}
	firstResponse := decodeCreateOrderResponse(t, firstRecorder.Body.Bytes())
	secondResponse := decodeCreateOrderResponse(t, secondRecorder.Body.Bytes())
	if secondResponse != firstResponse {
		t.Fatalf("second response = %+v, want first response %+v", secondResponse, firstResponse)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "idempotency_keys"); got != 1 {
		t.Fatalf("idempotency_keys count = %d, want 1", got)
	}
}

func TestOrderHandler_Create_sameIdempotencyKeyAndDifferentPayload_returnsConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newOrderHandlerTestDB(t, ctx, "http_order_idem_conflict")
	sut := &OrderHandler{Orders: store.NewOrderStore(testDB)}
	firstBody := []byte(`{"customer_id":"00000000-0000-0000-0000-000000000001","amount":"42.00"}`)
	secondBody := []byte(`{"customer_id":"00000000-0000-0000-0000-000000000001","amount":"43.00"}`)

	firstRequest := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(firstBody))
	firstRequest.Header.Set("Content-Type", "application/json")
	firstRequest.Header.Set(HeaderIdempotencyKey, "http-order-key-1")
	firstRecorder := httptest.NewRecorder()
	sut.Create(firstRecorder, firstRequest)

	secondRequest := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(secondBody))
	secondRequest.Header.Set("Content-Type", "application/json")
	secondRequest.Header.Set(HeaderIdempotencyKey, "http-order-key-1")
	secondRecorder := httptest.NewRecorder()
	sut.Create(secondRecorder, secondRequest)

	if firstRecorder.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", firstRecorder.Code)
	}
	if secondRecorder.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want 409", secondRecorder.Code)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}
	if got := countOrderHandlerRows(t, ctx, testDB, "idempotency_keys"); got != 1 {
		t.Fatalf("idempotency_keys count = %d, want 1", got)
	}
}

func decodeCreateOrderResponse(t *testing.T, body []byte) CreateOrderResponse {
	t.Helper()

	var response CreateOrderResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode create order response: %v", err)
	}
	return response
}

func newOrderHandlerTestDB(t *testing.T, ctx context.Context, prefix string) *sql.DB {
	t.Helper()

	baseURL := requireOrderHandlerTestDatabaseURL(t)
	adminDB, err := sql.Open("pgx", baseURL)
	if err != nil {
		t.Fatalf("open admin db: %v", err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })
	adminDB.SetMaxOpenConns(2)

	schema := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})

	testDB, err := sql.Open("pgx", withOrderHandlerSearchPath(baseURL, schema))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	testDB.SetMaxOpenConns(6)

	if err := applyOrderHandlerSchema(ctx, testDB); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return testDB
}

func requireOrderHandlerTestDatabaseURL(t *testing.T) string {
	t.Helper()
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("set RUN_DB_TESTS=1 to run PostgreSQL integration tests")
	}

	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		baseURL = os.Getenv("DATABASE_URL")
	}
	if baseURL == "" {
		baseURL = "postgres://postgres:postgres@localhost:55432/postgres?sslmode=disable"
	}
	return baseURL
}

func withOrderHandlerSearchPath(databaseURL, schema string) string {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	return u.String()
}

func applyOrderHandlerSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

		CREATE TABLE orders
		(
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			customer_id UUID NOT NULL,
			amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
			status      TEXT NOT NULL DEFAULT 'CREATED',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE outbox_events
		(
			id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			aggregate_type        TEXT NOT NULL,
			aggregate_id          UUID NOT NULL,
			event_type            TEXT NOT NULL,
			payload               JSONB NOT NULL,
			status                TEXT NOT NULL DEFAULT 'PENDING',
			attempt_count         INT NOT NULL DEFAULT 0,
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

		CREATE TABLE idempotency_keys
		(
			key             TEXT NOT NULL,
			endpoint        TEXT NOT NULL,
			request_hash    TEXT NOT NULL,
			response_status INTEGER NOT NULL,
			response_body   JSONB NOT NULL,
			order_id        UUID NOT NULL REFERENCES orders (id),
			event_id        UUID NOT NULL REFERENCES outbox_events (id),
			created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (endpoint, key)
		);
	`)
	return err
}

func countOrderHandlerRows(t *testing.T, ctx context.Context, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
