package config

import "os"

type Config struct {
	DatabaseURL      string
	HTTPAddr         string
	WebhookTargetURL string
}

func Load() Config {
	return Config{
		DatabaseURL:      getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		HTTPAddr:         getenv("HTTP_ADDR", ":8080"),
		WebhookTargetURL: getenv("WEBHOOK_TARGET_URL", "http://localhost:8080/mock/webhook"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
