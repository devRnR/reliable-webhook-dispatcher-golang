package store

import (
	"context"
	"database/sql"
)

type DeliveryAttempt struct {
	EventID      string
	ClaimToken   string
	TargetURL    string
	AttemptNo    int
	ResponseCode int
	ResponseBody string
	ErrorMessage string
	Success      bool
}

type DeliveryStore struct {
	DB *sql.DB
}

func NewDeliveryStore(db *sql.DB) *DeliveryStore {
	return &DeliveryStore{DB: db}
}

// InsertAttempt 는 주어진 tx 안에서 히도 1건을 기록한다.
// 상태 갱신과 같은 transaction 이어야 한다.
func (s *DeliveryStore) InsertAttempt(ctx context.Context, tx *sql.Tx, a DeliveryAttempt) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO delivery_attempts
				(event_id, claim_token, target_url, attempt_no, response_code, response_body, error_message, success)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		a.EventID, a.ClaimToken, a.TargetURL, a.AttemptNo,
		nullInt(a.ResponseCode), nullStr(a.ResponseBody), nullStr(a.ErrorMessage), a.Success)
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(i int) any {
	if i == 0 {
		return nil
	}
	return i
}
