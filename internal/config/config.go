// Package config loads the service configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the runtime configuration of the API service.
type Config struct {
	DatabaseURL     string
	WorkerCount     int
	QueueSize       int
	ShutdownTimeout time.Duration
	Addr            string
}

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Addr:        ":8080",
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	var err error
	if cfg.WorkerCount, err = intEnv("WORKER_COUNT", 4); err != nil {
		return Config{}, err
	}
	if cfg.WorkerCount < 1 {
		return Config{}, fmt.Errorf("WORKER_COUNT must be >= 1")
	}
	if cfg.QueueSize, err = intEnv("QUEUE_SIZE", 10000); err != nil {
		return Config{}, err
	}
	if cfg.QueueSize < 1 {
		return Config{}, fmt.Errorf("QUEUE_SIZE must be >= 1")
	}
	if cfg.ShutdownTimeout, err = durationEnv("SHUTDOWN_TIMEOUT", 25*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0")
	}
	return cfg, nil
}

func intEnv(key string, def int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}
