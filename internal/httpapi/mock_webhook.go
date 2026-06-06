package httpapi

import (
	"net/http"
	"time"
)

func MockWebhookReceiver(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("mode") {
	case "fail":
		http.Error(w, "simulated 500", http.StatusInternalServerError)
	case "bad":
		http.Error(w, "simulated 400", http.StatusBadRequest)
	case "timeout":
		time.Sleep(10 * time.Second)
		w.WriteHeader(http.StatusOK)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "ok"}`))
	}

}
