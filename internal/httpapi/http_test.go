package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	health(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status =- %d, want 200", rec.Code)
	}
}
