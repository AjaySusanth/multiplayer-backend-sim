package main

import (
	"context"
	"log/slog"
	"multiplayer-backend-sim/internal/matchmaking"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)


func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting matchmaking worker daemon")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/multiplayer_sim?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		// Unique identity per worker instance for Redis Consumer Group PEL tracking
		workerID = "worker-" + uuid.New().String()[:8]
	}

	ctx,cancel := context.WithCancel(context.Background())
	defer cancel()

	poolConfig,err :=pgxpool.ParseConfig(dbURL)
	if err != nil {
		logger.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}

	poolConfig.MaxConns = 10

	pgPool,err := pgxpool.NewWithConfig(ctx,poolConfig)
	if err != nil {
		logger.Error("failed to connect to database pool", "error", err)
		os.Exit(1)
	}

	defer pgPool.Close()

	if err:= pgPool.Ping(ctx);err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}

	logger.Info("connected to postgresql")

	redisOpts,err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Error("failed to parse redis url", "error", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()


	logger.Info("connected to redis")
	// 5. Initialize Stores and Consumer
	queueStore := matchmaking.NewPostgresQueueStore(pgPool)
	matchStore := matchmaking.NewPostgresMatchStore(pgPool)
	consumer := matchmaking.NewConsumer(
		redisClient,
		queueStore,
		matchStore,
		logger,
		workerID,
	)

	if err := consumer.SetupGroup(ctx); err != nil {
		logger.Error("failed to setup redis consumer group", "error", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal,1)
	signal.Notify(sigChan,os.Interrupt,syscall.SIGTERM)

	workerDone := make(chan struct{})
	go func ()  {
		defer close(workerDone)
		consumer.Run(ctx)
	}()

	logger.Info("matchmaking worker is running and processing jobs", "worker_id", workerID)
	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("shutdown signal received, draining worker...", "signal", sig.String())
	// Cancel context to initiate graceful shutdown
	cancel()


	select {
	case <-workerDone:
		logger.Info("worker loop terminated cleanly")
	case <-time.After(10 * time.Second):
		logger.Warn("worker shutdown timed out, forcing exit")
	}
	logger.Info("matchmaking worker shutdown complete")
}