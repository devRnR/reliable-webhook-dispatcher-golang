package httpapi

import (
	"net/http"
	"sync"
	"time"
)

type MockReceiver struct {
	mu   sync.Mutex
	seen map[string]int
}

func NewMockReceiver() *MockReceiver {
	return &MockReceiver{seen: make(map[string]int)}
}

func (m *MockReceiver) Handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("mode") {
	case "fail":
		http.Error(w, "simulated 500", http.StatusInternalServerError)
		return
	case "bad":
		http.Error(w, "simulated 400", http.StatusBadRequest)
		return
	case "timeout":
		time.Sleep(10 * time.Second)
	}
	id := r.Header.Get("Idempotency-Key")
	if id == "" {
		id = r.Header.Get("X-Event-ID")
	}
	if id != "" {
		m.mu.Lock()
		m.seen[id]++
		m.mu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok": true}`))
}

func (m *MockReceiver) Received(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	counts := make(map[string]int, len(m.seen))
	for k, v := range m.seen {
		counts[k] = v
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"distinct": len(counts), "counts": counts})
}
