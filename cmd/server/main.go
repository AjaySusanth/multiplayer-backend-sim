package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"multiplayer-backend-sim/internal/config"
	"multiplayer-backend-sim/internal/health"
	"multiplayer-backend-sim/internal/matchmaking"
	"multiplayer-backend-sim/internal/player"
	"multiplayer-backend-sim/internal/session"
)

func main() {
	// 1. Load runtime configuration
	cfg := config.Load()

	// 2. Initialize structured JSON logging via log/slog
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("starting multiplayer-backend-sim API server",
		"port", cfg.Port,
		"log_level", cfg.LogLevel,
	)

	// 3. Execute database schema migrations automatically on startup
	if err := runMigrations(cfg.DatabaseURL, logger); err != nil {
		logger.Error("failed to execute database migrations", "error", err)
		os.Exit(1)
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startupCancel()

	dbPool, err := pgxpool.New(startupCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("unable to initialize postgres connection pool", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(startupCtx); err != nil {
		logger.Error("failed to connect to postgresql database", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to postgresql database pool")

	// 5. Initialize Redis Client
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("failed to parse redis url", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(opt)
	defer redisClient.Close()
	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	logger.Info("successfully connected to redis")

	playerStore := player.NewPostgresPlayerStore(dbPool)
	queueStore := matchmaking.NewPostgresQueueStore(dbPool)
	matchStore := matchmaking.NewPostgresMatchStore(dbPool)
	sessionStore := session.NewPostgresSessionStore(dbPool)
	publisher := matchmaking.NewRedisQueuePublisher(redisClient)
	

	playerHandler := player.NewHandler(playerStore, logger)
	healthHandler := health.NewHandler(dbPool)
	matchmakingHandler := matchmaking.NewHandler(
		playerStore,
		queueStore,
		matchStore,
		publisher,
		logger,
	)
	sessionHandler := session.NewHandler(sessionStore,logger)

	consumer := matchmaking.NewConsumer(redisClient,queueStore,matchStore,logger)

	if err:= consumer.SetupGroup(startupCtx);err!=nil {
		logger.Error("failed to setup redis consumer group", "error", err)
		os.Exit(1)
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	go consumer.Run(workerCtx)

	// 6. Initialize Chi router and attach standard production middleware
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	playerHandler.RegisterRoutes(r)
	healthHandler.RegisterRoutes(r)
	matchmakingHandler.RegisterRoutes(r)
	sessionHandler.RegisterRoutes(r)

	// 7. Configure HTTP server timeouts
	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 8. Start HTTP server in a background goroutine
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	// 9. Listen for OS shutdown signals (SIGINT, SIGTERM)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error encountered", "error", err)
		}
	case sig := <-shutdown:
		logger.Info("shutdown signal received, initiating graceful shutdown", "signal", sig.String())

		workerCancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to gracefully shutdown HTTP server", "error", err)
			if err := server.Close(); err != nil {
				logger.Error("failed to force close server", "error", err)
			}
		}
		logger.Info("server shutdown complete")
	}
}

// runMigrations executes golang-migrate scripts from the migrations/ directory.
func runMigrations(databaseURL string, logger *slog.Logger) error {
	logger.Info("checking database migrations...")
	m, err := migrate.New("file://migrations", databaseURL)
	if err != nil {
		return fmt.Errorf("initializing migration engine: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("applying migrations: %w", err)
	}

	logger.Info("database migrations applied successfully")
	return nil
}
