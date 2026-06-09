package worker

import (
	"context"
	"errors"
	"log/slog"
	"reliable-webhook-dispatcher/internal/store"
	"time"
)

type Recoverer struct {
	outbox   *store.OutboxStore
	lease    time.Duration
	interval time.Duration
	logger   *slog.Logger
}

// NewRecoverer 는 필수 의존성을 검증하고 lease/interval 기본값을 채운다.
func NewRecoverer(outbox *store.OutboxStore, lease, interval time.Duration, logger *slog.Logger) (*Recoverer, error) {
	if outbox == nil {
		return nil, errors.New("recoverer: outbox is required")
	}
	if logger == nil {
		return nil, errors.New("recoverer: logger is required")
	}
	if lease <= 0 {
		lease = time.Minute
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Recoverer{outbox: outbox, lease: lease, interval: interval, logger: logger}, nil
}

func (r *Recoverer) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := r.outbox.RecoverStuck(ctx, r.lease)
			if err != nil {
				r.logger.Error("stuck recovery failed", "err", err)
				continue
			}
			if n > 0 {
				r.logger.Warn("stuck event recovered", "count", n)
			}
		}
	}
}
