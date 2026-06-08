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

type DeliveryAttemptRow struct {
	ID           int64  `json:"id"`
	EventID      string `json:"event_id"`
	AttemptNo    int    `json:"attempt_no"`
	ResponseCode int    `json:"response_code"`
	Success      bool   `json:"success"`
	ErrorMessage string `json:"error_message"`
	AttemptedAt  string `json:"attempted_at"`
}

func (s *DeliveryStore) List(ctx context.Context, limit int) ([]DeliveryAttemptRow, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, event_id, attempt_no, response_code, success, error_message, attempted_at
			   FROM delivery_attempts
			   ORDER BY attempted_at DESC
			   LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DeliveryAttemptRow, 0, limit)
	for rows.Next() {
		var r DeliveryAttemptRow
		var responseCode sql.NullInt64
		var errorMessage sql.NullString
		if err := rows.Scan(&r.ID, &r.EventID, &r.AttemptNo, &responseCode, &r.Success, &errorMessage, &r.AttemptedAt); err != nil {
			return nil, err
		}
		if responseCode.Valid {
			r.ResponseCode = int(responseCode.Int64)
		}
		if errorMessage.Valid {
			r.ErrorMessage = errorMessage.String
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
