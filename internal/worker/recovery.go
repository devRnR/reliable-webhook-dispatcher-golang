package worker

import (
	"context"
	"log/slog"
	"reliable-webhook-dispatcher/internal/store"
	"time"
)

type Recoverer struct {
	Outbox   *store.OutboxStore
	Lease    time.Duration
	Interval time.Duration
	Logger   *slog.Logger
}

func (r *Recoverer) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := r.Outbox.RecoverStuck(ctx, r.Lease)
			if err != nil {
				r.Logger.Error("stuck recovery failed", "err", err)
				continue
			}
			if n > 0 {
				r.Logger.Warn("stuck event recovered", "count", n)
			}
		}
	}
}
