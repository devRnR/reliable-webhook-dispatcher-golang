package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"reliable-webhook-dispatcher/internal/metrics"
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

func (p RetryPolicy) NextDelayJittered(attempt int) time.Duration {
	base := p.NextDelay(attempt)
	jitterMax := int64(base) / 5
	if jitterMax <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(jitterMax))
}

// 소비자 측(consumer-side) 인터페이스 — Dispatcher 가 실제로 필요한 행위만 좁게 정의한다.
// 구현은 store/HTTP 어댑터가 제공하고, 테스트는 가짜 구현으로 DispatchOnce 를 실DB/실HTTP 없이 검증한다.
type claimer interface {
	ClaimPending(ctx context.Context, limit int) ([]store.ClaimedEvent, error)
}

type completer interface {
	CompleteDelivery(ctx context.Context, att store.DeliveryAttempt, oc store.DeliveryOutcome, nextRetryAt time.Time) (int64, error)
}

type sender interface {
	Send(ctx context.Context, ev *store.ClaimedEvent) (status int, body string, err error)
	Target() string
}

type Dispatcher struct {
	claimer      claimer
	completer    completer
	sender       sender
	retry        RetryPolicy
	pollInterval time.Duration
	batchSize    int
	logger       *slog.Logger
	metrics      *metrics.Metrics
}

type DispatcherConfig struct {
	PollInterval time.Duration
	BatchSize    int
	Retry        RetryPolicy
}

type DispatcherDeps struct {
	Claimer   claimer
	Completer completer
	Sender    sender
	Logger    *slog.Logger
	Metrics   *metrics.Metrics
}

// NewDispatcher 는 필수 의존성을 검증하고 누락된 튜너블에 기본값을 채운 뒤 Dispatcher 를 만든다.
// export 필드 struct literal 조립과 달리 nil 의존성/0 값을 여기서 한 번에 막는다.
func NewDispatcher(cfg DispatcherConfig, deps DispatcherDeps) (*Dispatcher, error) {
	if deps.Claimer == nil {
		return nil, errors.New("dispatcher: Claimer is required")
	}
	if deps.Completer == nil {
		return nil, errors.New("dispatcher: Completer is required")
	}
	if deps.Sender == nil {
		return nil, errors.New("dispatcher: Sender is required")
	}
	if deps.Logger == nil {
		return nil, errors.New("dispatcher: Logger is required")
	}
	if deps.Metrics == nil {
		return nil, errors.New("dispatcher: Metrics is required")
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 5
	}

	return &Dispatcher{
		claimer:      deps.Claimer,
		completer:    deps.Completer,
		sender:       deps.Sender,
		retry:        cfg.Retry,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		logger:       deps.Logger,
		metrics:      deps.Metrics,
	}, nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.DispatchOnce(ctx); err != nil {
				d.logger.Error("dispatch 실패", "err", err)
			}
		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	events, err := d.claimer.ClaimPending(ctx, d.batchSize)
	if err != nil {
		return fmt.Errorf("dispatch once: %w", err)
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
		EventID: ev.ID, ClaimToken: ev.ClaimToken, TargetURL: d.sender.Target(), AttemptNo: attemptNo,
	}

	// 외부 호출은 트랜잭션 밖에서 끝낸다.
	start := time.Now()
	code, body, callErr := d.sender.Send(ctx, ev)
	d.metrics.DeliveryDuration.Observe(time.Since(start).Seconds())

	att.ResponseCode = code
	att.ResponseBody = body
	if callErr != nil {
		att.ErrorMessage = callErr.Error()
	}

	d.metrics.DeliveryAttempts.WithLabelValues(resultLabel(code, callErr)).Inc()

	oc := classify(code, callErr, attemptNo, d.retry.MaxAttempts)
	att.Success = oc == store.DeliverySent

	var nextRetryAt time.Time
	if oc == store.DeliveryRetry {
		nextRetryAt = time.Now().Add(d.retry.NextDelayJittered(attemptNo))
	}

	n, err := d.completer.CompleteDelivery(ctx, att, oc, nextRetryAt)
	if err != nil {
		d.logger.Error("delivery completion failed", "err", err, "event_id", att.EventID, "attempt_no", attemptNo)
		return
	}
	if n == 0 {
		// claim token guard 실패: 다른 워커/recovery 가 이미 이 이벤트를 가져갔다.
		d.logger.Warn("delivery completion: claim token lost", "event_id", att.EventID, "attempt_no", attemptNo)
	}
	d.metrics.EventsTotal.WithLabelValues(statusLabel(oc)).Inc()
}

func resultLabel(code int, callErr error) string {
	switch {
	case callErr != nil:
		return "error"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 400 && code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func statusLabel(oc store.DeliveryOutcome) string {
	switch oc {
	case store.DeliverySent:
		return "sent"
	case store.DeliveryRetry:
		return "retried"
	default:
		return "failed"
	}
}

func classify(code int, callErr error, attemptNo, maxAttempts int) store.DeliveryOutcome {
	switch {
	case callErr == nil && code >= 200 && code < 300:
		return store.DeliverySent
	case callErr == nil && code >= 400 && code < 500:
		return store.DeliveryFailed
	default:
		if attemptNo < maxAttempts {
			return store.DeliveryRetry
		}
		return store.DeliveryFailed
	}
}

// HTTPSender 는 webhook 전송 egress 어댑터다. Dispatcher 의 정책/오케스트레이션과 분리해
// 테스트에서 가짜 sender 로 교체할 수 있게 한다.
type HTTPSender struct {
	client    *http.Client
	targetURL string
}

func NewHTTPSender(targetURL string, client *http.Client) *HTTPSender {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &HTTPSender{client: client, targetURL: targetURL}
}

func (s *HTTPSender) Target() string { return s.targetURL }

func (s *HTTPSender) Send(ctx context.Context, ev *store.ClaimedEvent) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.targetURL, bytes.NewReader(ev.Payload))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", ev.ID)
	req.Header.Set("Idempotency-Key", ev.ID)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(b), nil
}
