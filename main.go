package main

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"

	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"reliable-webhook-dispatcher/internal/config"
	"reliable-webhook-dispatcher/internal/httpapi"
	"reliable-webhook-dispatcher/internal/store"
	"reliable-webhook-dispatcher/internal/worker"
	"syscall"
	"time"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if err := godotenv.Load(); err != nil {
		logger.Error("env 파일 로드 실패", "err", err)
	}
	cfg := config.Load()

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

	// HTTP 서버 시작
	mock := httpapi.NewMockReceiver()
	srv := httpapi.NewServer(cfg.HTTPAddr, db, mock)
	go func() {
		logger.Info("http 서버 시작", "addr", cfg.HTTPAddr)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http 서버 에러", "err", err)
			stop()
		}
	}()

	// Worker dispatcher 시작
	dispatcher := &worker.Dispatcher{
		DB:           db,
		Outbox:       store.NewOutboxStore(db),
		Delivery:     store.NewDeliveryStore(db),
		TargetUrl:    cfg.WebhookTargetURL,
		HTTPClient:   &http.Client{Timeout: 5 * time.Second},
		PollInterval: 2 * time.Second,
		Retry:        worker.RetryPolicy{MaxAttempts: 5},
		Logger:       logger,
		BatchSize:    10,
	}

	recoverer := &worker.Recoverer{
		Outbox:   store.NewOutboxStore(db),
		Lease:    1 * time.Minute,
		Interval: 30 * time.Second,
		Logger:   logger,
	}

	var wg sync.WaitGroup
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
