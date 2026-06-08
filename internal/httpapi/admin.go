package httpapi

import (
	"net/http"
	"reliable-webhook-dispatcher/internal/store"
)

type AdminHandler struct {
	Outbox *store.OutboxStore
}

func (h *AdminHandler) DeadLetters(w http.ResponseWriter, r *http.Request) {
	items, err := h.Outbox.ListDeadLetters(r.Context(), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list outbox failed")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *AdminHandler) Replay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("event_id")
	n, err := h.Outbox.ReplayOne(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "replay failed")
		return
	}
	if n == 0 {
		writeError(w, http.StatusConflict, "failed 상태가 아님")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"event_id": id, "status": "PENDING"})
}

func (h *AdminHandler) ReplayAll(w http.ResponseWriter, r *http.Request) {
	n, err := h.Outbox.ReplayAllFailed(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "replay all failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"replayed": n})
}
