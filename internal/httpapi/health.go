package httpapi

import (
	"database/sql"
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
)

type ReadyHandler struct {
	DB *sql.DB
}

func (h *ReadyHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.DB.PingContext(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "db not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type DeliveryHandler struct {
	Delivery *store.DeliveryStore
}

func (h *DeliveryHandler) List(w http.ResponseWriter, r *http.Request) {
	items, err := h.Delivery.List(r.Context(), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list delivery failed")
		return
	}
	writeJSON(w, http.StatusOK, items)
}
