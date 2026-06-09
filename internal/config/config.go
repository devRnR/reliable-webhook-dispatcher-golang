package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL        string
	HTTPAddr           string
	WebhookTargetURL   string
	EnableMockReceiver bool
	WorkerPollInterval time.Duration
	WorkerBatchSize    int
	RetryMaxAttempts   int
	RecovererLease     time.Duration
	RecovererInterval  time.Duration
}

func Load() (Config, error) {
	workerPollInterval, err := getDuration("WORKER_POLL_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}

	workerBatchSize, err := getInt("WORKER_BATCH_SIZE", 10)
	if err != nil {
		return Config{}, err
	}

	retryMaxAttempts, err := getInt("RETRY_MAX_ATTEMPTS", 5)
	if err != nil {
		return Config{}, err
	}

	recovererLease, err := getDuration("RECOVERER_LEASE", time.Minute)
	if err != nil {
		return Config{}, err
	}

	recovererInterval, err := getDuration("RECOVERER_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}

	enableMockReceiver, err := getBool("ENABLE_MOCK_RECEIVER", true)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		DatabaseURL:        getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		HTTPAddr:           getenv("HTTP_ADDR", ":8080"),
		WebhookTargetURL:   getenv("WEBHOOK_TARGET_URL", "http://localhost:8080/mock/webhook"),
		EnableMockReceiver: enableMockReceiver,
		WorkerPollInterval: workerPollInterval,
		WorkerBatchSize:    workerBatchSize,
		RetryMaxAttempts:   retryMaxAttempts,
		RecovererLease:     recovererLease,
		RecovererInterval:  recovererInterval,
	}

	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return d, nil
}

func getInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return n, nil
}

func getBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return b, nil
}
