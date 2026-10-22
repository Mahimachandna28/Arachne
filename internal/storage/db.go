package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps a PostgreSQL connection pool (pgxpool) and provides database operations.
type DB struct {
	pool *pgxpool.Pool
}

// PoolConfig holds connection pool settings for PostgreSQL.
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// DefaultPoolConfig returns recommended connection pool settings for crawler concurrency.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConns:        50,
		MinConns:        5,
		MaxConnLifetime: 1 * time.Hour,
		MaxConnIdleTime: 30 * time.Minute,
	}
}

// Connect initializes and validates a PostgreSQL connection pool.
func Connect(ctx context.Context, connStr string, poolCfg *PoolConfig) (*DB, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres connection string: %w", err)
	}

	cfg := DefaultPoolConfig()
	if poolCfg != nil {
		cfg = *poolCfg
	}

	if cfg.MaxConns > 0 {
		config.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		config.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		config.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		config.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Pool returns the underlying pgxpool.Pool for direct query execution.
func (d *DB) Pool() *pgxpool.Pool {
	return d.pool
}

// Ping checks if the database is accessible.
func (d *DB) Ping(ctx context.Context) error {
	return d.pool.Ping(ctx)
}

// Close gracefully closes the connection pool.
func (d *DB) Close() {
	if d.pool != nil {
		d.pool.Close()
	}
}

// InitSchema executes the initial schema DDL to create required tables and indexes.
func (d *DB) InitSchema(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, InitSchemaSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}
	return nil
}

// DropSchema executes the rollback DDL.
func (d *DB) DropSchema(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, DropSchemaSQL)
	if err != nil {
		return fmt.Errorf("failed to drop schema: %w", err)
	}
	return nil
}
