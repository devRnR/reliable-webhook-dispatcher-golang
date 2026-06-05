package httpapi

import "net/http"

func NewServer(add string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)

	return &http.Server{
		Addr:    add,
		Handler: mux,
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{ "status": "ok"}`))
}
