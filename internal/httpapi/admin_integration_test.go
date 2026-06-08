package httpapi

import (
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
)

func TestAdminHandler_DeadLetters_getReturnsFailedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newAdminTestDB(t, ctx, "admin_dead_letters")
	failedEventID := seedAdminFailedEvent(t, ctx, testDB)
	seedAdminPendingEvent(t, ctx, testDB)
	sut := NewServer(":0", testDB, NewMockReceiver()).Handler

	req := httptest.NewRequest(http.MethodGet, "/admin/dead-letters", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	var deadLetters []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&deadLetters); err != nil {
		t.Fatalf("decode dead letters: %v", err)
	}
	if len(deadLetters) != 1 {
		t.Fatalf("dead letters len = %d, want 1", len(deadLetters))
	}
	if deadLetters[0].ID != failedEventID || deadLetters[0].Status != "FAILED" {
		t.Fatalf("dead letter = %+v, want id=%s status=FAILED", deadLetters[0], failedEventID)
	}
}

func TestAdminHandler_Replay_postFailedEventMovesToPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newAdminTestDB(t, ctx, "admin_replay_one")
	failedEventID := seedAdminFailedEvent(t, ctx, testDB)
	sut := NewServer(":0", testDB, NewMockReceiver()).Handler

	req := httptest.NewRequest(http.MethodPost, "/admin/outbox/"+failedEventID+"/replay", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if response.EventID != failedEventID || response.Status != "PENDING" {
		t.Fatalf("response = %+v, want event_id=%s status=PENDING", response, failedEventID)
	}
	assertAdminEventStatus(t, ctx, testDB, failedEventID, "PENDING")
}

func TestAdminHandler_Replay_postNonFailedEventReturnsConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newAdminTestDB(t, ctx, "admin_replay_conflict")
	pendingEventID := seedAdminPendingEvent(t, ctx, testDB)
	sut := NewServer(":0", testDB, NewMockReceiver()).Handler

	req := httptest.NewRequest(http.MethodPost, "/admin/outbox/"+pendingEventID+"/replay", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 body=%s", rec.Code, rec.Body.String())
	}
	assertAdminEventStatus(t, ctx, testDB, pendingEventID, "PENDING")
}

func TestAdminHandler_ReplayAll_postMovesAllFailedEventsToPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newAdminTestDB(t, ctx, "admin_replay_all")
	seedAdminFailedEvent(t, ctx, testDB)
	seedAdminFailedEvent(t, ctx, testDB)
	seedAdminPendingEvent(t, ctx, testDB)
	sut := NewServer(":0", testDB, NewMockReceiver()).Handler

	req := httptest.NewRequest(http.MethodPost, "/admin/dead-letters/replay", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Replayed int64 `json:"replayed"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode replay all response: %v", err)
	}
	if response.Replayed != 2 {
		t.Fatalf("replayed = %d, want 2", response.Replayed)
	}
	if got := countAdminRows(t, ctx, testDB, "outbox_events", "status = 'FAILED'"); got != 0 {
		t.Fatalf("failed count = %d, want 0", got)
	}
	if got := countAdminRows(t, ctx, testDB, "outbox_events", "status = 'PENDING'"); got != 3 {
		t.Fatalf("pending count = %d, want 3", got)
	}
}

func newAdminTestDB(t *testing.T, ctx context.Context, prefix string) *sql.DB {
	t.Helper()

	baseURL := requireAdminTestDatabaseURL(t)
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

	testDB, err := sql.Open("pgx", withAdminSearchPath(baseURL, schema))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	testDB.SetMaxOpenConns(4)

	if err := applyAdminSchema(ctx, testDB); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return testDB
}

func requireAdminTestDatabaseURL(t *testing.T) string {
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

func withAdminSearchPath(databaseURL, schema string) string {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	return u.String()
}

func applyAdminSchema(ctx context.Context, db *sql.DB) error {
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
	`)
	return err
}

func seedAdminFailedEvent(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()

	var eventID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status, attempt_count, last_error)
		VALUES
			('order', gen_random_uuid(), 'order.created', '{"ok":true}', 'FAILED', 5, 'delivery failed')
		RETURNING id
	`).Scan(&eventID); err != nil {
		t.Fatalf("seed failed event: %v", err)
	}
	return eventID
}

func seedAdminPendingEvent(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()

	var eventID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status)
		VALUES
			('order', gen_random_uuid(), 'order.created', '{"ok":true}', 'PENDING')
		RETURNING id
	`).Scan(&eventID); err != nil {
		t.Fatalf("seed pending event: %v", err)
	}
	return eventID
}

func assertAdminEventStatus(t *testing.T, ctx context.Context, db *sql.DB, eventID string, want string) {
	t.Helper()

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status); err != nil {
		t.Fatalf("query event status: %v", err)
	}
	if status != want {
		t.Fatalf("status = %s, want %s", status, want)
	}
}

func countAdminRows(t *testing.T, ctx context.Context, db *sql.DB, table string, where string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&count); err != nil {
		t.Fatalf("count %s where %s: %v", table, where, err)
	}
	return count
}
