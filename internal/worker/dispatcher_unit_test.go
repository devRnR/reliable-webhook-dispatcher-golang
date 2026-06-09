package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	appmetrics "reliable-webhook-dispatcher/internal/metrics"
	"reliable-webhook-dispatcher/internal/store"
)

// 이 파일은 R5(소비자측 인터페이스) 덕분에 가능한 단위테스트다.
// 실DB/실HTTP 없이 가짜 claimer/sender/completer 로 DispatchOnce 의 분기 정책만 검증한다.

type fakeClaimer struct {
	events []store.ClaimedEvent
	err    error
}

func (f *fakeClaimer) ClaimPending(ctx context.Context, limit int) ([]store.ClaimedEvent, error) {
	return f.events, f.err
}

type fakeSender struct {
	status int
	body   string
	err    error
	sent   int
}

func (f *fakeSender) Target() string { return "http://fake.test/webhook" }

func (f *fakeSender) Send(ctx context.Context, ev *store.ClaimedEvent) (int, string, error) {
	f.sent++
	return f.status, f.body, f.err
}

type recordedCompletion struct {
	att  store.DeliveryAttempt
	oc   store.DeliveryOutcome
	next time.Time
}

type fakeCompleter struct {
	completions []recordedCompletion
	n           int64
	err         error
}

func (f *fakeCompleter) CompleteDelivery(ctx context.Context, att store.DeliveryAttempt, oc store.DeliveryOutcome, nextRetryAt time.Time) (int64, error) {
	f.completions = append(f.completions, recordedCompletion{att: att, oc: oc, next: nextRetryAt})
	if f.err != nil {
		return 0, f.err
	}
	return f.n, nil
}

func newUnitDispatcher(t *testing.T, c claimer, s sender, comp completer) *Dispatcher {
	t.Helper()
	d, err := NewDispatcher(
		DispatcherConfig{PollInterval: time.Hour, BatchSize: 10, Retry: RetryPolicy{MaxAttempts: 5}},
		DispatcherDeps{
			Claimer:   c,
			Sender:    s,
			Completer: comp,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Metrics:   appmetrics.New(prometheus.NewRegistry()),
		},
	)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	return d
}

func oneEvent(attemptCount int) []store.ClaimedEvent {
	return []store.ClaimedEvent{{ID: "e1", ClaimToken: "t1", Payload: []byte(`{}`), AttemptCount: attemptCount}}
}

func TestDispatchOnce_2xx_completesAsSent(t *testing.T) {
	sender := &fakeSender{status: 200, body: "ok"}
	completer := &fakeCompleter{n: 1}
	d := newUnitDispatcher(t, &fakeClaimer{events: oneEvent(0)}, sender, completer)

	if err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	if sender.sent != 1 {
		t.Fatalf("sender.sent = %d, want 1", sender.sent)
	}
	if len(completer.completions) != 1 {
		t.Fatalf("completions = %d, want 1", len(completer.completions))
	}
	c := completer.completions[0]
	if c.oc != store.DeliverySent {
		t.Fatalf("outcome = %v, want DeliverySent", c.oc)
	}
	if c.att.EventID != "e1" || c.att.AttemptNo != 1 || !c.att.Success {
		t.Fatalf("att = %+v, want e1/attempt1/success", c.att)
	}
	if !c.next.IsZero() {
		t.Fatalf("nextRetryAt = %v, want zero for sent", c.next)
	}
}

func TestDispatchOnce_5xx_completesAsRetryWithBackoff(t *testing.T) {
	completer := &fakeCompleter{n: 1}
	d := newUnitDispatcher(t, &fakeClaimer{events: oneEvent(0)}, &fakeSender{status: 500}, completer)

	if err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	c := completer.completions[0]
	if c.oc != store.DeliveryRetry {
		t.Fatalf("outcome = %v, want DeliveryRetry", c.oc)
	}
	if c.next.IsZero() {
		t.Fatalf("nextRetryAt is zero, want a backoff time for retry")
	}
}

func TestDispatchOnce_4xx_completesAsFailedWithoutRetry(t *testing.T) {
	completer := &fakeCompleter{n: 1}
	d := newUnitDispatcher(t, &fakeClaimer{events: oneEvent(0)}, &fakeSender{status: 400}, completer)

	if err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	c := completer.completions[0]
	if c.oc != store.DeliveryFailed {
		t.Fatalf("outcome = %v, want DeliveryFailed", c.oc)
	}
	if !c.next.IsZero() {
		t.Fatalf("nextRetryAt = %v, want zero for failed", c.next)
	}
}

func TestDispatchOnce_5xxAtMaxAttempts_failsWithoutRetry(t *testing.T) {
	// attemptCount=4 → attemptNo=5 == MaxAttempts → 더 이상 재시도하지 않고 FAILED
	completer := &fakeCompleter{n: 1}
	d := newUnitDispatcher(t, &fakeClaimer{events: oneEvent(4)}, &fakeSender{status: 503}, completer)

	if err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	if completer.completions[0].oc != store.DeliveryFailed {
		t.Fatalf("outcome = %v, want DeliveryFailed at max attempts", completer.completions[0].oc)
	}
}

func TestDispatchOnce_transportError_retriesWithNullResponseCode(t *testing.T) {
	completer := &fakeCompleter{n: 1}
	d := newUnitDispatcher(t, &fakeClaimer{events: oneEvent(0)}, &fakeSender{err: errors.New("connection refused")}, completer)

	if err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch once: %v", err)
	}
	c := completer.completions[0]
	if c.oc != store.DeliveryRetry {
		t.Fatalf("outcome = %v, want DeliveryRetry on transport error", c.oc)
	}
	if c.att.ResponseCode != 0 || c.att.ErrorMessage == "" {
		t.Fatalf("att = %+v, want response_code=0 and non-empty error", c.att)
	}
}

func TestDispatchOnce_claimError_propagates(t *testing.T) {
	d := newUnitDispatcher(t, &fakeClaimer{err: errors.New("db down")}, &fakeSender{}, &fakeCompleter{})
	if err := d.DispatchOnce(context.Background()); err == nil {
		t.Fatalf("want error propagated from claim failure")
	}
}

func TestNewDispatcher_missingDeps_returnsError(t *testing.T) {
	if _, err := NewDispatcher(DispatcherConfig{}, DispatcherDeps{}); err == nil {
		t.Fatalf("want error for missing required deps")
	}
}
