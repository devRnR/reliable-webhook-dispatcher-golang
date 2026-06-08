package httpapi

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	appmetrics "reliable-webhook-dispatcher/internal/metrics"
)

func TestServer_Metrics_exposesWebhookMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := appmetrics.New(reg)
	m.EventsTotal.WithLabelValues("sent").Inc()
	m.DeliveryAttempts.WithLabelValues("2xx").Inc()
	m.DeliveryDuration.Observe(0.01)
	m.Backlog.WithLabelValues("pending").Set(3)

	sut := NewServer(":0", nil, NewMockReceiver(), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), discardLogger()).Handler

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, metricName := range []string{
		"webhook_outbox_events_total",
		"webhook_delivery_attempts_total",
		"webhook_delivery_duration_seconds",
		"webhook_outbox_backlog",
	} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("/metrics body does not contain %s:\n%s", metricName, body)
		}
	}
	if strings.Contains(body, "event_id") || strings.Contains(body, "claim_token") || strings.Contains(body, "request_id") {
		t.Fatalf("/metrics body contains high-cardinality labels:\n%s", body)
	}
}

func TestRequestLogger_successfulRequest_logsJSONFields(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	sut := RequestLogger(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/orders", nil)
	rec := httptest.NewRecorder()
	sut.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}

	var logLine map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logLine); err != nil {
		t.Fatalf("decode slog json: %v body=%q", err, buf.String())
	}
	if logLine["msg"] != "http" || logLine["method"] != http.MethodPost || logLine["path"] != "/orders" {
		t.Fatalf("log line = %+v, want msg/http method/path", logLine)
	}
	if logLine["status"] != float64(http.StatusCreated) {
		t.Fatalf("status field = %v, want 201", logLine["status"])
	}
	if _, ok := logLine["dur_ms"]; !ok {
		t.Fatalf("log line has no dur_ms: %+v", logLine)
	}
	requestID, ok := logLine["request_id"].(string)
	if !ok || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(requestID) {
		t.Fatalf("request_id = %v, want 16-char hex", logLine["request_id"])
	}
}

func newHTTPTestHandler(db *sql.DB) http.Handler {
	reg := prometheus.NewRegistry()
	appmetrics.New(reg)
	return NewServer(":0", db, NewMockReceiver(), promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), discardLogger()).Handler
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
