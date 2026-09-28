package config

import (
	"os"
	"strconv"
)

// Config holds all runtime settings for the application, loaded from environment variables.
type Config struct {
	Port        string
	DatabaseURL string
	RedisURL    string
	LogLevel    string
	WorkerCount int
}

// Load reads configuration from environment variables, falling back to sensible defaults for local development.
func Load() *Config {
	return &Config{
		Port:        getEnv("PORT", "8080"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/multiplayer_sim?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "localhost:6379"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		WorkerCount: getEnvAsInt("WORKER_COUNT", 3),
	}
}

// getEnv retrieves an environment variable or returns a fallback default.
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

// getEnvAsInt parses an environment variable into an integer or returns a fallback default.
func getEnvAsInt(key string, fallback int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return fallback
}
