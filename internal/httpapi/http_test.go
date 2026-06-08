package httpapi

import (
	"encoding/json"
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

func TestMockReceiver_ReceivedCountsDuplicateEventDelivery(t *testing.T) {
	receiver := NewMockReceiver()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/mock/webhook", nil)
		req.Header.Set("X-Event-ID", "event-1")
		rec := httptest.NewRecorder()

		receiver.Handle(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("mock webhook status = %d, want 200", rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/mock/webhook/received", nil)
	rec := httptest.NewRecorder()
	receiver.Received(rec, req)

	var got struct {
		Distinct int            `json:"distinct"`
		Counts   map[string]int `json:"counts"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode received response: %v", err)
	}
	if got.Distinct != 1 || got.Counts["event-1"] != 2 {
		t.Fatalf("received = %+v, want distinct=1 count[event-1]=2", got)
	}
}

func TestMockReceiver_ReceivedCountsDuplicateIdempotencyKeyDelivery(t *testing.T) {
	receiver := NewMockReceiver()

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/mock/webhook", nil)
		req.Header.Set(HeaderIdempotencyKey, "event-1")
		rec := httptest.NewRecorder()

		receiver.Handle(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("mock webhook status = %d, want 200", rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/mock/webhook/received", nil)
	rec := httptest.NewRecorder()
	receiver.Received(rec, req)

	var got struct {
		Distinct int            `json:"distinct"`
		Counts   map[string]int `json:"counts"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode received response: %v", err)
	}
	if got.Distinct != 1 || got.Counts["event-1"] != 2 {
		t.Fatalf("received = %+v, want distinct=1 count[event-1]=2", got)
	}
}
