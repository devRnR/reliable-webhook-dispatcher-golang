package httpapi

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
)

func NewServer(add string, db *sql.DB, mock *MockReceiver, metricsHandler http.Handler, logger *slog.Logger) *http.Server {

	orderH := &OrderHandler{Orders: store.NewOrderStore(db)}
	outboxH := &OutboxHandler{Outbox: store.NewOutboxStore(db)}
	readyH := &ReadyHandler{DB: db}
	deliveryH := &DeliveryHandler{Delivery: store.NewDeliveryStore(db)}
	adminH := &AdminHandler{Outbox: store.NewOutboxStore(db)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", readyH.Ready)
	mux.Handle("GET /metrics", metricsHandler)
	mux.HandleFunc("POST /orders", orderH.Create)
	mux.HandleFunc("GET /outbox", outboxH.List)
	mux.HandleFunc("GET /outbox/stats", outboxH.Stats)
	mux.HandleFunc("GET /delivery-attempts", deliveryH.List)
	mux.HandleFunc("POST /mock/webhook", mock.Handle)
	mux.HandleFunc("GET /mock/webhook/received", mock.Received)
	mux.HandleFunc("GET /admin/dead-letters", adminH.DeadLetters)
	mux.HandleFunc("POST /admin/dead-letters/replay", adminH.ReplayAll)
	mux.HandleFunc("POST /admin/outbox/{event_id}/replay", adminH.Replay)

	return &http.Server{Addr: add, Handler: RequestLogger(logger, mux)}
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
