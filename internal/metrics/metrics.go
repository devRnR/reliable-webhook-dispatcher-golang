package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	EventsTotal      *prometheus.CounterVec
	DeliveryAttempts *prometheus.CounterVec
	DeliveryDuration prometheus.Histogram
	Backlog          *prometheus.GaugeVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_outbox_events_total",
				Help: "outbox event 상태 전이 횟수",
			}, []string{"status"}),
		DeliveryAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "webhook_delivery_attempts_total",
				Help: "webhook 전송 시도 횟수",
			}, []string{"result"}),
		DeliveryDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "webhook_delivery_duration_seconds",
				Help:    "webhook 전송 소요 시간(초)",
				Buckets: prometheus.DefBuckets,
			}),
		Backlog: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "webhook_outbox_backlog",
				Help: "status 별 outbox 적체",
			}, []string{"status"}),
	}
	reg.MustRegister(m.EventsTotal, m.DeliveryAttempts, m.DeliveryDuration, m.Backlog)
	return m
}
