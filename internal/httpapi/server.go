package httpapi

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
)

// ServerDeps 는 main(composition root)이 한 번 조립한 의존성을 NewServer 로 주입하기 위한 묶음이다.
// 예전처럼 NewServer 안에서 *sql.DB 로 store 를 다시 만들지 않는다(이중 조립 제거).
type ServerDeps struct {
	DB             *sql.DB // /ready health probe 용
	Order          *store.OrderStore
	Outbox         *store.OutboxStore
	Delivery       *store.DeliveryStore
	Mock           *MockReceiver
	MetricsHandler http.Handler
	EnableMock     bool // mock webhook 라우트는 데모/로컬에서만 마운트
}

func NewServer(addr string, deps ServerDeps, logger *slog.Logger) *http.Server {
	orderH := &OrderHandler{Orders: deps.Order}
	outboxH := &OutboxHandler{Outbox: deps.Outbox}
	readyH := &ReadyHandler{DB: deps.DB}
	deliveryH := &DeliveryHandler{Delivery: deps.Delivery}
	adminH := &AdminHandler{Outbox: deps.Outbox}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", readyH.Ready)
	if deps.MetricsHandler != nil {
		mux.Handle("GET /metrics", deps.MetricsHandler)
	}
	mux.HandleFunc("POST /orders", orderH.Create)
	mux.HandleFunc("GET /outbox", outboxH.List)
	mux.HandleFunc("GET /outbox/stats", outboxH.Stats)
	mux.HandleFunc("GET /delivery-attempts", deliveryH.List)
	if deps.EnableMock && deps.Mock != nil {
		mux.HandleFunc("POST /mock/webhook", deps.Mock.Handle)
		mux.HandleFunc("GET /mock/webhook/received", deps.Mock.Received)
	}
	mux.HandleFunc("GET /admin/dead-letters", adminH.DeadLetters)
	mux.HandleFunc("POST /admin/dead-letters/replay", adminH.ReplayAll)
	mux.HandleFunc("POST /admin/outbox/{event_id}/replay", adminH.Replay)

	return &http.Server{Addr: addr, Handler: RequestLogger(logger, mux)}
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{ "status": "ok"}`))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
