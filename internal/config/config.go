package config

import "os"

type Config struct {
	DatabaseURL string
	HTTPAddr    string
}

func Load() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
