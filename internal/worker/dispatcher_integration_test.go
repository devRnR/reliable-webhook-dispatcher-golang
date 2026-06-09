package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	appmetrics "reliable-webhook-dispatcher/internal/metrics"
	"reliable-webhook-dispatcher/internal/store"
)

func TestDispatcher_DispatchOnce_2xxMarksSentAndRecordsAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newDispatcherTestDB(t, ctx, "dispatch_success")
	eventID := seedDispatcherPendingEvent(t, ctx, testDB, 0)

	var received int32
	var idempotencyKeyReceived int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Event-ID") == eventID {
			atomic.AddInt32(&received, 1)
		}
		if r.Header.Get("Idempotency-Key") == eventID {
			atomic.AddInt32(&idempotencyKeyReceived, 1)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)

	dispatcher := newTestDispatcher(testDB, server.URL, &http.Client{Timeout: time.Second})
	if err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}

	status, attemptCount, claimTokenValid, nextRetryValid := outboxState(t, ctx, testDB, eventID)
	if status != store.OutboxStatusSent || attemptCount != 1 || claimTokenValid || nextRetryValid {
		t.Fatalf("state=%s attempt=%d claim_token_valid=%v next_retry_valid=%v, want SENT/1/null/null",
			status, attemptCount, claimTokenValid, nextRetryValid)
	}
	if atomic.LoadInt32(&received) != 1 {
		t.Fatalf("receiver count = %d, want 1", received)
	}
	if atomic.LoadInt32(&idempotencyKeyReceived) != 1 {
		t.Fatalf("idempotency key count = %d, want 1", idempotencyKeyReceived)
	}
	assertDeliveryAttempt(t, ctx, testDB, eventID, 1, 200, true, false)
	if got := counterValue(t, dispatcher.metrics.DeliveryAttempts.WithLabelValues("2xx")); got != 1 {
		t.Fatalf("delivery_attempts{result=2xx} = %v, want 1", got)
	}
	if got := counterValue(t, dispatcher.metrics.EventsTotal.WithLabelValues("sent")); got != 1 {
		t.Fatalf("outbox_events{status=sent} = %v, want 1", got)
	}
}

func TestDispatcher_DispatchOnce_5xxKeepsPendingWithBackoffAndAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newDispatcherTestDB(t, ctx, "dispatch_5xx")
	eventID := seedDispatcherPendingEvent(t, ctx, testDB, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "simulated 500", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	dispatcher := newTestDispatcher(testDB, server.URL, &http.Client{Timeout: time.Second})
	if err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}

	status, attemptCount, claimTokenValid, nextRetryValid := outboxState(t, ctx, testDB, eventID)
	if status != store.OutboxStatusPending || attemptCount != 1 || claimTokenValid || !nextRetryValid {
		t.Fatalf("state=%s attempt=%d claim_token_valid=%v next_retry_valid=%v, want PENDING/1/null/non-null",
			status, attemptCount, claimTokenValid, nextRetryValid)
	}
	assertDeliveryAttempt(t, ctx, testDB, eventID, 1, 500, false, false)
	if got := counterValue(t, dispatcher.metrics.DeliveryAttempts.WithLabelValues("5xx")); got != 1 {
		t.Fatalf("delivery_attempts{result=5xx} = %v, want 1", got)
	}
	if got := counterValue(t, dispatcher.metrics.EventsTotal.WithLabelValues("retried")); got != 1 {
		t.Fatalf("outbox_events{status=retried} = %v, want 1", got)
	}
}

func TestDispatcher_DispatchOnce_4xxMarksFailedWithoutRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newDispatcherTestDB(t, ctx, "dispatch_4xx")
	eventID := seedDispatcherPendingEvent(t, ctx, testDB, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "simulated 400", http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)

	dispatcher := newTestDispatcher(testDB, server.URL, &http.Client{Timeout: time.Second})
	if err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}

	status, attemptCount, claimTokenValid, nextRetryValid := outboxState(t, ctx, testDB, eventID)
	if status != store.OutboxStatusFailed || attemptCount != 1 || claimTokenValid || nextRetryValid {
		t.Fatalf("state=%s attempt=%d claim_token_valid=%v next_retry_valid=%v, want FAILED/1/null/null",
			status, attemptCount, claimTokenValid, nextRetryValid)
	}
	assertDeliveryAttempt(t, ctx, testDB, eventID, 1, 400, false, false)
}

func TestDispatcher_DispatchOnce_timeoutDoesNotBlockWorkerIndefinitely(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newDispatcherTestDB(t, ctx, "dispatch_timeout")
	eventID := seedDispatcherPendingEvent(t, ctx, testDB, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	dispatcher := newTestDispatcher(testDB, server.URL, &http.Client{Timeout: 100 * time.Millisecond})

	start := time.Now()
	if err := dispatcher.DispatchOnce(ctx); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 700*time.Millisecond {
		t.Fatalf("dispatch elapsed = %s, want bounded by client timeout", elapsed)
	}

	status, attemptCount, claimTokenValid, nextRetryValid := outboxState(t, ctx, testDB, eventID)
	if status != store.OutboxStatusPending || attemptCount != 1 || claimTokenValid || !nextRetryValid {
		t.Fatalf("state=%s attempt=%d claim_token_valid=%v next_retry_valid=%v, want PENDING/1/null/non-null",
			status, attemptCount, claimTokenValid, nextRetryValid)
	}
	assertDeliveryAttempt(t, ctx, testDB, eventID, 1, 0, false, true)
}

func TestDispatcher_RunCanceledContextReturnsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dispatcher := &Dispatcher{pollInterval: time.Hour}
	if err := dispatcher.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
}

func TestRetryPolicy_NextDelayJittered_attemptOneStaysWithinTwentyPercentJitter(t *testing.T) {
	sut := RetryPolicy{MaxAttempts: 5}

	for i := 0; i < 100; i++ {
		delay := sut.NextDelayJittered(1)
		if delay < 10*time.Second || delay >= 12*time.Second {
			t.Fatalf("delay = %s, want [10s, 12s)", delay)
		}
	}
}

func newTestDispatcher(db *sql.DB, targetURL string, client *http.Client) *Dispatcher {
	d, err := NewDispatcher(
		DispatcherConfig{
			PollInterval: time.Hour,
			BatchSize:    10,
			Retry:        RetryPolicy{MaxAttempts: 5},
		},
		DispatcherDeps{
			Claimer:   store.NewOutboxStore(db),
			Completer: store.NewDeliveryCompleter(db),
			Sender:    NewHTTPSender(targetURL, client),
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Metrics:   appmetrics.New(prometheus.NewRegistry()),
		},
	)
	if err != nil {
		panic(err)
	}
	return d
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()

	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func newDispatcherTestDB(t *testing.T, ctx context.Context, prefix string) *sql.DB {
	t.Helper()

	baseURL := requireDispatcherTestDatabaseURL(t)
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

	testDB, err := sql.Open("pgx", withDispatcherSearchPath(baseURL, schema))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	testDB.SetMaxOpenConns(10)

	if err := applyDispatcherSchema(ctx, testDB); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return testDB
}

func requireDispatcherTestDatabaseURL(t *testing.T) string {
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

func withDispatcherSearchPath(databaseURL, schema string) string {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	return u.String()
}

func applyDispatcherSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

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

		CREATE INDEX idx_outbox_pending
			ON outbox_events (status, next_retry_at, created_at) WHERE status = 'PENDING';

		CREATE TABLE delivery_attempts
		(
			id            BIGSERIAL PRIMARY KEY,
			event_id      UUID NOT NULL REFERENCES outbox_events (id),
			claim_token   UUID NOT NULL,
			target_url    TEXT NOT NULL,
			attempt_no    INT NOT NULL,
			response_code INT,
			response_body TEXT,
			success       BOOLEAN NOT NULL,
			error_message TEXT,
			attempted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (event_id, claim_token, attempt_no)
		);
	`)
	return err
}

func seedDispatcherPendingEvent(t *testing.T, ctx context.Context, db *sql.DB, attemptCount int) string {
	t.Helper()

	var eventID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status, attempt_count)
		VALUES
			('order', gen_random_uuid(), 'order.created', '{"ok":true}', 'PENDING', $1)
		RETURNING id
	`, attemptCount).Scan(&eventID); err != nil {
		t.Fatalf("seed pending event: %v", err)
	}
	return eventID
}

func outboxState(t *testing.T, ctx context.Context, db *sql.DB, eventID string) (status string, attemptCount int, claimTokenValid bool, nextRetryValid bool) {
	t.Helper()

	var claimToken sql.NullString
	var nextRetryAt sql.NullTime
	if err := db.QueryRowContext(ctx,
		`SELECT status, attempt_count, claim_token, next_retry_at FROM outbox_events WHERE id = $1`, eventID).
		Scan(&status, &attemptCount, &claimToken, &nextRetryAt); err != nil {
		t.Fatalf("query outbox state: %v", err)
	}
	return status, attemptCount, claimToken.Valid, nextRetryAt.Valid
}

func assertDeliveryAttempt(t *testing.T, ctx context.Context, db *sql.DB, eventID string, attemptNo int, responseCode int, success bool, wantError bool) {
	t.Helper()

	var gotAttemptNo int
	var gotResponseCode sql.NullInt64
	var gotSuccess bool
	var gotError sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT attempt_no, response_code, success, error_message
		   FROM delivery_attempts WHERE event_id = $1`, eventID).
		Scan(&gotAttemptNo, &gotResponseCode, &gotSuccess, &gotError); err != nil {
		t.Fatalf("query delivery attempt: %v", err)
	}
	if gotAttemptNo != attemptNo {
		t.Fatalf("attempt_no = %d, want %d", gotAttemptNo, attemptNo)
	}
	if responseCode == 0 {
		if gotResponseCode.Valid {
			t.Fatalf("response_code = %d, want NULL", gotResponseCode.Int64)
		}
	} else if !gotResponseCode.Valid || int(gotResponseCode.Int64) != responseCode {
		t.Fatalf("response_code = %v/%d, want %d", gotResponseCode.Valid, gotResponseCode.Int64, responseCode)
	}
	if gotSuccess != success {
		t.Fatalf("success = %v, want %v", gotSuccess, success)
	}
	if gotError.Valid != wantError {
		t.Fatalf("error valid = %v, want %v", gotError.Valid, wantError)
	}
}
