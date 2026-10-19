//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestHarness encapsulates running test containers and client pools.
type TestHarness struct {
	PGPool        *pgxpool.Pool
	RedisClient   *redis.Client
	pgContainer   *tcpostgres.PostgresContainer
	redisContainer *tcredis.RedisContainer
}

// SetupHarness spins up clean Postgres and Redis containers, applies all database
// migrations, and returns ready-to-use client pools along with a teardown closure.
func SetupHarness(ctx context.Context) (*TestHarness, func(), error) {
	// 1. Start PostgreSQL Container
	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("starting postgres container: %w", err)
	}

	pgConnStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("getting postgres connection string: %w", err)
	}

	// 2. Start Redis Container
	redisContainer, err := tcredis.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		pgContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("starting redis container: %w", err)
	}

	redisURI, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		pgContainer.Terminate(ctx)
		redisContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("getting redis connection string: %w", err)
	}

	// 3. Apply SQL Migrations to the test database
	if err := applyMigrations(pgConnStr); err != nil {
		pgContainer.Terminate(ctx)
		redisContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("applying migrations: %w", err)
	}

	// 4. Initialize real pgxpool connection
	poolConfig, err := pgxpool.ParseConfig(pgConnStr)
	if err != nil {
		pgContainer.Terminate(ctx)
		redisContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("parsing pgxpool config: %w", err)
	}
	poolConfig.MaxConns = 10

	pgPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		pgContainer.Terminate(ctx)
		redisContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("creating pgxpool: %w", err)
	}

	// 5. Initialize real Redis client
	redisOpts, err := redis.ParseURL(redisURI)
	if err != nil {
		pgPool.Close()
		pgContainer.Terminate(ctx)
		redisContainer.Terminate(ctx)
		return nil, nil, fmt.Errorf("parsing redis url: %w", err)
	}
	redisClient := redis.NewClient(redisOpts)

	harness := &TestHarness{
		PGPool:         pgPool,
		RedisClient:    redisClient,
		pgContainer:    pgContainer,
		redisContainer: redisContainer,
	}

	// Teardown closure: cleanly terminates connection pools and docker containers
	teardown := func() {
		pgPool.Close()
		redisClient.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = pgContainer.Terminate(cleanupCtx)
		_ = redisContainer.Terminate(cleanupCtx)
	}

	return harness, teardown, nil
}

// applyMigrations applies all .sql files from the migrations directory using golang-migrate.
func applyMigrations(connStr string) error {
	// Find absolute path to migrations folder relative to this file
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("failed to get caller path")
	}
	// Walk up from tests/integration to repo root, then into migrations/
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations")
	migrationSource := fmt.Sprintf("file://%s", filepath.ToSlash(migrationsDir))

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("opening db for migration: %w", err)
	}
	defer db.Close()

	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return fmt.Errorf("creating migrate driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(migrationSource, "postgres", driver)
	if err != nil {
		return fmt.Errorf("initializing migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("running migrations up: %w", err)
	}

	return nil
}