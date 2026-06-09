package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DeliveryOutcome 은 webhook 전송 1회의 결과를 트랜잭션 경계 안에서 어떻게
// 반영할지 나타낸다. 재시도 지연 같은 정책 계산은 worker(도메인)가 하고,
// 여기로는 결과 분류만 넘어온다.
type DeliveryOutcome int

const (
	DeliverySent DeliveryOutcome = iota
	DeliveryRetry
	DeliveryFailed
)

// DeliveryCompleter 는 "delivery attempt 기록 + outbox 상태 전이"를 한 트랜잭션으로
// 묶는 unit of work 의 소유자다. worker 는 이 한 메서드만 호출하고 *sql.Tx 를 직접
// 다루지 않는다. 외부 HTTP 호출은 이 트랜잭션 밖에서 끝난 뒤 들어온다.
type DeliveryCompleter struct {
	db       *sql.DB
	outbox   *OutboxStore
	delivery *DeliveryStore
}

func NewDeliveryCompleter(db *sql.DB) *DeliveryCompleter {
	return &DeliveryCompleter{
		db:       db,
		outbox:   NewOutboxStore(db),
		delivery: NewDeliveryStore(db),
	}
}

// CompleteDelivery 는 att 를 기록하고 oc 에 따라 outbox 행을 전이시킨다.
// nextRetryAt 은 DeliveryRetry 에서만 쓰인다. 반환값은 outbox 영향행 수로,
// 0 이면 claim_token guard 로 다른 워커/recovery 가 이미 가져간 것 → 커밋하지 않고
// 롤백한다(이 경우 attempt 기록도 함께 롤백되어 stale 기록을 남기지 않는다).
func (c *DeliveryCompleter) CompleteDelivery(ctx context.Context, att DeliveryAttempt, oc DeliveryOutcome, nextRetryAt time.Time) (int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := c.delivery.InsertAttempt(ctx, tx, att); err != nil {
		return 0, fmt.Errorf("insert delivery attempt: %w", err)
	}

	var n int64
	switch oc {
	case DeliverySent:
		n, err = c.outbox.MarkSentTx(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo)
	case DeliveryRetry:
		n, err = c.outbox.MarkRetryTx(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo, nextRetryAt, att.ErrorMessage)
	case DeliveryFailed:
		msg := att.ErrorMessage
		if msg == "" {
			msg = "delivery failed"
		}
		n, err = c.outbox.MarkFailed(ctx, tx, att.EventID, att.ClaimToken, att.AttemptNo, msg)
	default:
		return 0, fmt.Errorf("unknown delivery outcome %d", oc)
	}
	if err != nil {
		return 0, fmt.Errorf("mark outcome: %w", err)
	}
	if n == 0 {
		// claim token guard 실패 — 커밋하지 않고 defer 의 Rollback 에 맡긴다.
		return 0, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}
