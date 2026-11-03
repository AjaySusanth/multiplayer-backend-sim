package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name         string
		envVars      map[string]string
		assertConfig func(t *testing.T, cfg *Config)
	}{
		{
			name:    "Default values when environment variables are unset",
			envVars: map[string]string{},
			assertConfig: func(t *testing.T, cfg *Config) {
				if cfg.Port != "8080" {
					t.Errorf("expected default Port 8080, got %s", cfg.Port)
				}
				if cfg.LogLevel != "info" {
					t.Errorf("expected default LogLevel info, got %s", cfg.LogLevel)
				}
				if cfg.WorkerCount != 3 {
					t.Errorf("expected default WorkerCount 3, got %d", cfg.WorkerCount)
				}
				if !strings.HasPrefix(cfg.WorkerID, "worker-") {
					t.Errorf("expected default WorkerID to start with 'worker-', got %s", cfg.WorkerID)
				}
				if cfg.InternalSecret != "dev-internal-secret" {
					t.Errorf("expected default InternalSecret 'dev-internal-secret', got %s", cfg.InternalSecret)
				}
			},
		},
		{
			name: "Explicit WORKER_ID override is preserved",
			envVars: map[string]string{
				"PORT":            "9090",
				"LOG_LEVEL":       "debug",
				"WORKER_COUNT":    "5",
				"WORKER_ID":       "worker-custom-test-1",
				"INTERNAL_SECRET": "prod-secret-test",
			},
			assertConfig: func(t *testing.T, cfg *Config) {
				if cfg.Port != "9090" {
					t.Errorf("expected Port 9090, got %s", cfg.Port)
				}
				if cfg.LogLevel != "debug" {
					t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
				}
				if cfg.WorkerCount != 5 {
					t.Errorf("expected WorkerCount 5, got %d", cfg.WorkerCount)
				}
				if cfg.WorkerID != "worker-custom-test-1" {
					t.Errorf("expected WorkerID 'worker-custom-test-1', got %s", cfg.WorkerID)
				}
				if cfg.InternalSecret != "prod-secret-test" {
					t.Errorf("expected InternalSecret 'prod-secret-test', got %s", cfg.InternalSecret)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Clear relevant env vars before and restore after
			keys := []string{"PORT", "DATABASE_URL", "REDIS_URL", "LOG_LEVEL", "WORKER_COUNT", "WORKER_ID"}
			for _, k := range keys {
				originalVal, wasSet := os.LookupEnv(k)
				defer func(key, val string, restore bool) {
					if restore {
						_ = os.Setenv(key, val)
					} else {
						_ = os.Unsetenv(key)
					}
				}(k, originalVal, wasSet)
				_ = os.Unsetenv(k)
			}

			// Apply testcase env vars
			for k, v := range tc.envVars {
				_ = os.Setenv(k, v)
			}

			cfg := Load()
			tc.assertConfig(t, cfg)
		})
	}
}
