package httpapi

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
)

func NewServer(add string, db *sql.DB) *http.Server {

	orderH := &OrderHandler{Orders: store.NewOrderStore(db)}
	outboxH := &OutboxHandler{Outbox: store.NewOutboxStore(db)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("POST /orders", orderH.Create)
	mux.HandleFunc("POST /mock/webhook", MockWebhookReceiver)
	mux.HandleFunc("GET /outbox", outboxH.List)
	mux.HandleFunc("GET /outbox/stats", outboxH.Stats)

	return &http.Server{Addr: add, Handler: mux}
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
