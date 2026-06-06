package worker

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
	"time"
)

type Dispatcher struct {
	Store        *store.OutboxStore
	TargetUrl    string
	HTTPClient   *http.Client
	PollInterval time.Duration
	Logger       *slog.Logger
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
	ev, err := d.Store.ClaimNext(ctx)
	if err != nil {
		return err
	}
	if ev == nil {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetUrl, bytes.NewReader(ev.Payload))
	if err != nil {
		return d.Store.ReleaseToPending(ctx, ev.ID, ev.ClaimToken)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", ev.ID)

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		d.Logger.Warn("webhook 호출 실패", "event_id", ev.ID, "err", err)
		return d.Store.ReleaseToPending(ctx, ev.ID, ev.ClaimToken)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		d.Logger.Info("webhook 전송 성공", "event_id", ev.ID, "status", resp.StatusCode)
		return d.Store.MarkSent(ctx, ev.ID, ev.ClaimToken)
	}

	d.Logger.Warn("webhook 비2xx", "event_id", ev.ID, "status", resp.StatusCode)
	return d.Store.ReleaseToPending(ctx, ev.ID, ev.ClaimToken)

}
