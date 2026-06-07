package worker

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
}

func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 10 * time.Second
	case 2:
		return 30 * time.Second
	case 3:
		return 1 * time.Minute
	default:
		return 5 * time.Minute
	}
}

type Dispatcher struct {
	DB           *sql.DB
	Outbox       *store.OutboxStore
	Delivery     *store.DeliveryStore
	TargetUrl    string
	HTTPClient   *http.Client
	PollInterval time.Duration
	Retry        RetryPolicy
	Logger       *slog.Logger
	BatchSize    int
}

func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); err != nil {
				d.Logger.Error("dispatch 실패", "err", err)
			}

		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	events, err := d.Outbox.ClaimPending(ctx, d.BatchSize)
	if err != nil {
		return err
	}
	for i := range events {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.dispatchEvent(ctx, &events[i])
	}

	return nil
}

func (d *Dispatcher) dispatchEvent(ctx context.Context, ev *store.ClaimedEvent) {
	attemptNo := ev.AttemptCount + 1
	att := store.DeliveryAttempt{
		EventID: ev.ID, ClaimToken: ev.ClaimToken, TargetURL: d.TargetUrl, AttemptNo: attemptNo,
	}

	// 외부 호출은 transaction 밖
	code, body, callErr := d.send(ctx, ev)
	att.ResponseCode = code
	att.ResponseBody = body

	if callErr != nil {
		att.ErrorMessage = callErr.Error()
	}

	oc := classify(code, callErr, attemptNo, d.Retry.MaxAttempts)
	att.Success = oc == outSent
	if err := d.complete(ctx, att, oc); err != nil {
		d.Logger.Error("delivery failed", "err", err)
	}
}

func (d *Dispatcher) send(ctx context.Context, ev *store.ClaimedEvent) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetUrl, bytes.NewReader(ev.Payload))
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", ev.ID)

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(b), nil
}

type outcome int

const (
	outSent outcome = iota
	outRetry
	outFailed
)

func classify(code int, callErr error, attemptNo, maxAttempts int) outcome {
	switch {
	case callErr == nil && code >= 200 && code < 300:
		return outSent
	case callErr == nil && code >= 400 && code < 500:
		return outFailed
	default:
		if attemptNo < maxAttempts {
			return outRetry
		}
		return outFailed
	}
}

func (d *Dispatcher) complete(ctx context.Context, att store.DeliveryAttempt, oc outcome) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := d.Delivery.InsertAttempt(ctx, tx, att); err != nil {
		return err
	}

	var n int64
	switch oc {
	case outSent:
		n, err = d.Outbox.MarkSentTx(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo)
	case outRetry:
		next := time.Now().Add(d.Retry.NextDelay(att.AttemptNo))
		n, err = d.Outbox.MarkRetryTx(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo, next, att.ErrorMessage)
	case outFailed:
		msg := att.ErrorMessage
		if msg == "" {
			msg = "delivery failed"
		}
		n, err = d.Outbox.MarkFailed(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo, msg)
	}
	if err != nil {
		return err
	}
	if n == 0 {
		d.Logger.Warn("delivery failed", "event_id", att.EventID, "attempt_no", att.AttemptNo)
		return tx.Rollback()
	}
	return tx.Commit()
}
