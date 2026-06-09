package main

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"reliable-webhook-dispatcher/internal/config"
	"reliable-webhook-dispatcher/internal/httpapi"
	"reliable-webhook-dispatcher/internal/metrics"
	"reliable-webhook-dispatcher/internal/store"
	"reliable-webhook-dispatcher/internal/worker"
	"syscall"
	"time"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	if err := godotenv.Load(); err != nil {
		logger.Error("env 파일 로드 실패", "err", err)
	}
	cfg, err := config.Load()
	if err != nil {
		logger.Error("config 로드 실패", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		logger.Error("db open 실패", "err", err)
		os.Exit(1)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logger.Error("db close 실패", "err", err)
		}
	}(db)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		logger.Error("db ping 실패", "err", err)
		os.Exit(1)
	}

	logger.Info("db 연결 성공")

	// composition root: store 는 여기서 한 번만 만들어 모든 소비자에 주입한다.
	outboxStore := store.NewOutboxStore(db)
	orderStore := store.NewOrderStore(db)
	deliveryStore := store.NewDeliveryStore(db)
	deliveryCompleter := store.NewDeliveryCompleter(db)

	var wg sync.WaitGroup

	// backlog metrics 수집 goroutine (shutdown 동기화를 위해 WaitGroup 에 포함)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st, err := outboxStore.Stats(ctx)
				if err != nil {
					continue
				}
				m.Backlog.WithLabelValues("pending").Set(float64(st.Pending))
				m.Backlog.WithLabelValues("processing").Set(float64(st.Processing))
				m.Backlog.WithLabelValues("failed").Set(float64(st.Failed))
			}
		}
	}()

	// HTTP 서버 시작
	mock := httpapi.NewMockReceiver()
	metricsHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	srv := httpapi.NewServer(cfg.HTTPAddr, httpapi.ServerDeps{
		DB:             db,
		Order:          orderStore,
		Outbox:         outboxStore,
		Delivery:       deliveryStore,
		Mock:           mock,
		MetricsHandler: metricsHandler,
		EnableMock:     cfg.EnableMockReceiver,
	}, logger)
	go func() {
		logger.Info("http 서버 시작", "addr", cfg.HTTPAddr)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http 서버 에러", "err", err)
			stop()
		}
	}()

	// Worker dispatcher 시작
	dispatcher, err := worker.NewDispatcher(
		worker.DispatcherConfig{
			PollInterval: cfg.WorkerPollInterval,
			BatchSize:    cfg.WorkerBatchSize,
			Retry:        worker.RetryPolicy{MaxAttempts: cfg.RetryMaxAttempts},
		},
		worker.DispatcherDeps{
			Claimer:   outboxStore,
			Completer: deliveryCompleter,
			Sender:    worker.NewHTTPSender(cfg.WebhookTargetURL, &http.Client{Timeout: 5 * time.Second}),
			Logger:    logger,
			Metrics:   m,
		},
	)
	if err != nil {
		logger.Error("dispatcher 생성 실패", "err", err)
		os.Exit(1)
	}

	recoverer, err := worker.NewRecoverer(outboxStore, cfg.RecovererLease, cfg.RecovererInterval, logger)
	if err != nil {
		logger.Error("recoverer 생성 실패", "err", err)
		os.Exit(1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("dispatcher 종료", "err", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := recoverer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("recoverer finished", "err", err)
		}
	}()

	// 종료 시그널 대기
	// SIGINT, SIGTERM 이 오거나 goroutine 에서 stop() 이 호출되면,
	// ctx 가 취소되고 <- ctx.Done() 이 리턴된다.
	<-ctx.Done()
	logger.Info("종료 신호 수신 - graceful shutdown 시작")

	// 이때 ctx 는 이미 cancel 된 상태이기 때문에, 그걸 부모로 쓰면 바로 취소돼서 graceful 하게
	// 기다릴 수가 없다.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown 실패", "err", err)
	}

	wg.Wait()
	logger.Info("종료 완료")
}
