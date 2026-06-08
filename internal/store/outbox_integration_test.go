package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestOutboxStore_ClaimPending_concurrentWorkersDoNotOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "claim_pending", 10)
	if err := seedPendingEvents(ctx, testDB, 50); err != nil {
		t.Fatalf("seed pending events: %v", err)
	}

	sut := NewOutboxStore(testDB)
	start := make(chan struct{})
	claimedByWorker := make(chan []ClaimedEvent, 5)
	errs := make(chan error, 5)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			events, err := sut.ClaimPending(ctx, 10)
			if err != nil {
				errs <- err
				return
			}
			claimedByWorker <- events
		}()
	}

	close(start)
	wg.Wait()
	close(claimedByWorker)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("claim pending: %v", err)
		}
	}

	claimedIDs := make(map[string]struct{})
	totalClaimed := 0
	for events := range claimedByWorker {
		if len(events) > 10 {
			t.Fatalf("worker claimed %d events, want <= 10", len(events))
		}

		for _, event := range events {
			if event.ClaimToken == "" {
				t.Fatalf("event %s has empty claim_token", event.ID)
			}
			if _, exists := claimedIDs[event.ID]; exists {
				t.Fatalf("event %s claimed more than once", event.ID)
			}
			claimedIDs[event.ID] = struct{}{}
			totalClaimed++
		}
	}

	if totalClaimed > 50 {
		t.Fatalf("total claimed = %d, want <= 50", totalClaimed)
	}
}

func TestOrderStore_CreateOrderWithOutbox_commitsOrderAndOutboxTogether(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_outbox_commit", 2)

	res, err := NewOrderStore(testDB).CreateOrderWithOutbox(ctx, CreateOrderInput{
		CustomerID: "00000000-0000-0000-0000-000000000001",
		Amount:     "120.50",
	})
	if err != nil {
		t.Fatalf("create order with outbox: %v", err)
	}
	if res.OrderID == "" || res.EventID == "" || res.Status != "CREATED" {
		t.Fatalf("unexpected result: %+v", res)
	}

	if got := countRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}

	var status string
	var attemptCount int
	if err := testDB.QueryRowContext(ctx,
		`SELECT status, attempt_count FROM outbox_events WHERE id = $1`, res.EventID).
		Scan(&status, &attemptCount); err != nil {
		t.Fatalf("query outbox event: %v", err)
	}
	if status != OutboxStatusPending || attemptCount != 0 {
		t.Fatalf("outbox status=%s attempt_count=%d, want PENDING/0", status, attemptCount)
	}
}

func TestOrderStore_CreateOrderWithOutbox_rollsBackOrderWhenOutboxInsertFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_outbox_rollback", 2)
	if _, err := testDB.ExecContext(ctx,
		`ALTER TABLE outbox_events ADD CONSTRAINT reject_order_created CHECK (event_type <> 'order.created')`); err != nil {
		t.Fatalf("add failing outbox constraint: %v", err)
	}

	_, err := NewOrderStore(testDB).CreateOrderWithOutbox(ctx, CreateOrderInput{
		CustomerID: "00000000-0000-0000-0000-000000000001",
		Amount:     "120.50",
	})
	if err == nil {
		t.Fatal("create order with outbox succeeded, want outbox insert failure")
	}

	if got := countRows(t, ctx, testDB, "orders"); got != 0 {
		t.Fatalf("orders count = %d, want 0 after rollback", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 0 {
		t.Fatalf("outbox_events count = %d, want 0 after rollback", got)
	}
}

func TestOrderStore_CreateOrderWithOutbox_sameIdempotencyKeyAndRequestHash_reusesCachedResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_idem_existing", 4)
	sut := NewOrderStore(testDB)
	firstOrder := CreateOrderInput{
		CustomerID:     "00000000-0000-0000-0000-000000000001",
		Amount:         "120.50",
		IdempotencyKey: "order-key-1",
		RequestHash:    "same-request-hash",
		Endpoint:       "POST /orders",
	}

	created, err := sut.CreateOrderWithOutbox(ctx, firstOrder)
	if err != nil {
		t.Fatalf("create first order: %v", err)
	}
	replayed, err := sut.CreateOrderWithOutbox(ctx, firstOrder)
	if err != nil {
		t.Fatalf("replay order: %v", err)
	}

	if created.Outcome != IdemReserved {
		t.Fatalf("created outcome = %v, want IdemReserved", created.Outcome)
	}
	if replayed.Outcome != IdemExisting || replayed.CachedStatus != 201 {
		t.Fatalf("replayed outcome/status = %v/%d, want IdemExisting/201", replayed.Outcome, replayed.CachedStatus)
	}

	var cached struct {
		OrderID string `json:"order_id"`
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(replayed.CachedBody, &cached); err != nil {
		t.Fatalf("decode cached body: %v", err)
	}
	if cached.OrderID != created.OrderID || cached.EventID != created.EventID || cached.Status != "CREATED" {
		t.Fatalf("cached body = %+v, want first order/event", cached)
	}
	if got := countRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "idempotency_keys"); got != 1 {
		t.Fatalf("idempotency_keys count = %d, want 1", got)
	}
}

func TestOrderStore_CreateOrderWithOutbox_sameIdempotencyKeyAndDifferentRequestHash_returnsConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_idem_conflict", 4)
	sut := NewOrderStore(testDB)

	_, err := sut.CreateOrderWithOutbox(ctx, CreateOrderInput{
		CustomerID:     "00000000-0000-0000-0000-000000000001",
		Amount:         "120.50",
		IdempotencyKey: "order-key-1",
		RequestHash:    "first-request-hash",
		Endpoint:       "POST /orders",
	})
	if err != nil {
		t.Fatalf("create first order: %v", err)
	}
	conflict, err := sut.CreateOrderWithOutbox(ctx, CreateOrderInput{
		CustomerID:     "00000000-0000-0000-0000-000000000001",
		Amount:         "130.50",
		IdempotencyKey: "order-key-1",
		RequestHash:    "different-request-hash",
		Endpoint:       "POST /orders",
	})
	if err != nil {
		t.Fatalf("create conflicting order: %v", err)
	}

	if conflict.Outcome != IdemConflict {
		t.Fatalf("conflict outcome = %v, want IdemConflict", conflict.Outcome)
	}
	if got := countRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "idempotency_keys"); got != 1 {
		t.Fatalf("idempotency_keys count = %d, want 1", got)
	}
}

func TestOrderStore_CreateOrderWithOutbox_emptyIdempotencyKey_createsNewOrderEveryTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_idem_empty", 4)
	sut := NewOrderStore(testDB)
	order := CreateOrderInput{
		CustomerID: "00000000-0000-0000-0000-000000000001",
		Amount:     "120.50",
	}

	if _, err := sut.CreateOrderWithOutbox(ctx, order); err != nil {
		t.Fatalf("create first order: %v", err)
	}
	if _, err := sut.CreateOrderWithOutbox(ctx, order); err != nil {
		t.Fatalf("create second order: %v", err)
	}

	if got := countRows(t, ctx, testDB, "orders"); got != 2 {
		t.Fatalf("orders count = %d, want 2", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 2 {
		t.Fatalf("outbox_events count = %d, want 2", got)
	}
	if got := countRows(t, ctx, testDB, "idempotency_keys"); got != 0 {
		t.Fatalf("idempotency_keys count = %d, want 0", got)
	}
}

func TestOrderStore_CreateOrderWithOutbox_concurrentSameIdempotencyKey_createsOneOrderAndOneOutboxEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "order_idem_concurrent", 12)
	sut := NewOrderStore(testDB)
	order := CreateOrderInput{
		CustomerID:     "00000000-0000-0000-0000-000000000001",
		Amount:         "120.50",
		IdempotencyKey: "concurrent-order-key",
		RequestHash:    "same-request-hash",
		Endpoint:       "POST /orders",
	}

	start := make(chan struct{})
	errs := make(chan error, 10)
	outcomes := make(chan IdemOutcome, 10)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			created, err := sut.CreateOrderWithOutbox(ctx, order)
			if err != nil {
				errs <- err
				return
			}
			outcomes <- created.Outcome
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	close(outcomes)

	for err := range errs {
		if err != nil {
			t.Fatalf("create concurrent order: %v", err)
		}
	}

	reservedCount := 0
	existingCount := 0
	for outcome := range outcomes {
		switch outcome {
		case IdemReserved:
			reservedCount++
		case IdemExisting:
			existingCount++
		default:
			t.Fatalf("unexpected outcome = %v", outcome)
		}
	}
	if reservedCount != 1 || existingCount != 9 {
		t.Fatalf("reserved/existing = %d/%d, want 1/9", reservedCount, existingCount)
	}
	if got := countRows(t, ctx, testDB, "orders"); got != 1 {
		t.Fatalf("orders count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "outbox_events"); got != 1 {
		t.Fatalf("outbox_events count = %d, want 1", got)
	}
	if got := countRows(t, ctx, testDB, "idempotency_keys"); got != 1 {
		t.Fatalf("idempotency_keys count = %d, want 1", got)
	}
}

func TestOutboxStore_RecoverStuck_releasesExpiredProcessingRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "recover_stuck", 2)
	eventID, _ := seedProcessingEvent(t, ctx, testDB, 2*time.Minute)

	n, err := NewOutboxStore(testDB).RecoverStuck(ctx, time.Minute)
	if err != nil {
		t.Fatalf("recover stuck: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered rows = %d, want 1", n)
	}

	var status string
	var claimToken sql.NullString
	var processingStartedAt sql.NullTime
	var lastError sql.NullString
	if err := testDB.QueryRowContext(ctx,
		`SELECT status, claim_token, processing_started_at, last_error
		   FROM outbox_events WHERE id = $1`, eventID).
		Scan(&status, &claimToken, &processingStartedAt, &lastError); err != nil {
		t.Fatalf("query recovered event: %v", err)
	}
	if status != OutboxStatusPending || claimToken.Valid || processingStartedAt.Valid {
		t.Fatalf("status=%s claim_token_valid=%v processing_started_at_valid=%v, want PENDING/null/null",
			status, claimToken.Valid, processingStartedAt.Valid)
	}
	if !lastError.Valid || lastError.String != "recovered from stuck processing" {
		t.Fatalf("last_error = %q, want recovery marker", lastError.String)
	}
}

func TestOutboxStore_MarkSentTxWithStaleClaimToken_returnsZeroAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "stale_claim", 2)
	eventID, staleToken := seedProcessingEvent(t, ctx, testDB, 2*time.Minute)

	n, err := NewOutboxStore(testDB).RecoverStuck(ctx, time.Minute)
	if err != nil {
		t.Fatalf("recover stuck: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered rows = %d, want 1", n)
	}

	tx, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	n, err = NewOutboxStore(testDB).MarkSentTx(ctx, tx, eventID, staleToken, 1)
	if err != nil {
		t.Fatalf("mark sent with stale token: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows affected = %d, want 0 for stale claim_token", n)
	}

	var status string
	if err := testDB.QueryRowContext(ctx, `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status); err != nil {
		t.Fatalf("query event status: %v", err)
	}
	if status != OutboxStatusPending {
		t.Fatalf("status = %s, want PENDING", status)
	}
}

func TestOutboxStore_ReplayOne_failedEvent_movesToPendingAndPreservesAttemptCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "replay_one_failed", 2)
	eventID := seedFailedEvent(t, ctx, testDB, 5)
	sut := NewOutboxStore(testDB)

	replayed, err := sut.ReplayOne(ctx, eventID)
	if err != nil {
		t.Fatalf("replay one: %v", err)
	}

	if replayed != 1 {
		t.Fatalf("replayed rows = %d, want 1", replayed)
	}

	var status string
	var attemptCount int
	var nextRetryAt sql.NullTime
	var claimToken sql.NullString
	var processingStartedAt sql.NullTime
	if err := testDB.QueryRowContext(ctx,
		`SELECT status, attempt_count, next_retry_at, claim_token, processing_started_at
		   FROM outbox_events WHERE id = $1`, eventID).
		Scan(&status, &attemptCount, &nextRetryAt, &claimToken, &processingStartedAt); err != nil {
		t.Fatalf("query replayed event: %v", err)
	}
	if status != OutboxStatusPending {
		t.Fatalf("status = %s, want PENDING", status)
	}
	if attemptCount != 5 {
		t.Fatalf("attempt_count = %d, want preserved value 5", attemptCount)
	}
	if !nextRetryAt.Valid {
		t.Fatal("next_retry_at is NULL, want replay to be immediately claimable")
	}
	if claimToken.Valid || processingStartedAt.Valid {
		t.Fatalf("claim_token_valid=%v processing_started_at_valid=%v, want null/null",
			claimToken.Valid, processingStartedAt.Valid)
	}
}

func TestOutboxStore_ReplayOne_nonFailedEvent_returnsZeroAndDoesNotChangeStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "replay_one_non_failed", 2)
	eventID := seedPendingEvent(t, ctx, testDB)
	sut := NewOutboxStore(testDB)

	replayed, err := sut.ReplayOne(ctx, eventID)
	if err != nil {
		t.Fatalf("replay one: %v", err)
	}

	if replayed != 0 {
		t.Fatalf("replayed rows = %d, want 0", replayed)
	}

	var status string
	if err := testDB.QueryRowContext(ctx, `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if status != OutboxStatusPending {
		t.Fatalf("status = %s, want unchanged PENDING", status)
	}
}

func TestOutboxStore_ReplayAllFailed_onlyFailedEventsMoveToPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "replay_all_failed", 2)
	seedFailedEvent(t, ctx, testDB, 3)
	seedFailedEvent(t, ctx, testDB, 4)
	seedPendingEvent(t, ctx, testDB)
	sut := NewOutboxStore(testDB)

	replayed, err := sut.ReplayAllFailed(ctx)
	if err != nil {
		t.Fatalf("replay all failed: %v", err)
	}

	if replayed != 2 {
		t.Fatalf("replayed rows = %d, want 2", replayed)
	}
	stats, err := sut.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Pending != 3 || stats.Failed != 0 {
		t.Fatalf("stats = %+v, want pending=3 failed=0", stats)
	}
}

func TestOutboxStore_ListDeadLetters_onlyReturnsFailedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testDB := newIsolatedTestDB(t, ctx, "list_dead_letters", 2)
	seedFailedEvent(t, ctx, testDB, 3)
	seedFailedEvent(t, ctx, testDB, 4)
	seedPendingEvent(t, ctx, testDB)

	deadLetters, err := NewOutboxStore(testDB).ListDeadLetters(ctx, 100)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}

	if len(deadLetters) != 2 {
		t.Fatalf("dead letters len = %d, want 2", len(deadLetters))
	}
	for _, event := range deadLetters {
		if event.Status != OutboxStatusFailed {
			t.Fatalf("dead letter status = %s, want FAILED", event.Status)
		}
	}
}

func newIsolatedTestDB(t *testing.T, ctx context.Context, prefix string, maxOpenConns int) *sql.DB {
	t.Helper()

	baseURL := requireTestDatabaseURL(t)
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

	testDB, err := sql.Open("pgx", withSearchPath(baseURL, schema))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { _ = testDB.Close() })
	testDB.SetMaxOpenConns(maxOpenConns)

	if err := applyOutboxSchema(ctx, testDB); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return testDB
}

func requireTestDatabaseURL(t *testing.T) string {
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

func withSearchPath(databaseURL, schema string) string {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}
	q := u.Query()
	q.Set("search_path", schema+",public")
	u.RawQuery = q.Encode()
	return u.String()
}

func applyOutboxSchema(ctx context.Context, db *sql.DB) error {
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

		CREATE INDEX idx_outbox_pending
			ON outbox_events (status, next_retry_at, created_at) WHERE status = 'PENDING';

		CREATE INDEX idx_outbox_processing_started
			ON outbox_events (status, processing_started_at) WHERE status = 'PROCESSING';

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

		CREATE INDEX idx_delivery_attempts_event
			ON delivery_attempts (event_id, attempted_at DESC);

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

func seedPendingEvents(ctx context.Context, db *sql.DB, count int) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status, created_at)
		SELECT
			'order',
			gen_random_uuid(),
			'order.created',
			jsonb_build_object('sequence', gs),
			'PENDING',
			now() + (gs * interval '1 millisecond')
		FROM generate_series(1, $1) AS gs
	`, count)
	return err
}

func seedProcessingEvent(t *testing.T, ctx context.Context, db *sql.DB, age time.Duration) (eventID string, claimToken string) {
	t.Helper()

	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status, claim_token, processing_started_at)
		VALUES
			('order', gen_random_uuid(), 'order.created', '{"ok":true}', 'PROCESSING',
			 gen_random_uuid(), now() - make_interval(secs => $1))
		RETURNING id, claim_token
	`, int(age.Seconds())).Scan(&eventID, &claimToken); err != nil {
		t.Fatalf("seed processing event: %v", err)
	}
	return eventID, claimToken
}

func seedFailedEvent(t *testing.T, ctx context.Context, db *sql.DB, attemptCount int) string {
	t.Helper()

	var eventID string
	if err := db.QueryRowContext(ctx, `
		INSERT INTO outbox_events
			(aggregate_type, aggregate_id, event_type, payload, status, attempt_count, last_error, claim_token, processing_started_at)
		VALUES
			('order', gen_random_uuid(), 'order.created', '{"ok":true}', 'FAILED', $1,
			 'delivery failed', gen_random_uuid(), now() - interval '1 minute')
		RETURNING id
	`, attemptCount).Scan(&eventID); err != nil {
		t.Fatalf("seed failed event: %v", err)
	}
	return eventID
}

func seedPendingEvent(t *testing.T, ctx context.Context, db *sql.DB) string {
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

func countRows(t *testing.T, ctx context.Context, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
