package app

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MoFadel26/logengine/internal/config"
	"github.com/MoFadel26/logengine/internal/pgtest"
)

// TestReadinessFailsDuringShutdown checks the first step of the shutdown
// order: readiness must report 503 once the service is winding down, while
// liveness stays green.
func TestReadinessFailsDuringShutdown(t *testing.T) {
	dsn := pgtest.Start(t)

	a, err := New(context.Background(), config.Config{
		DatabaseURL:     dsn,
		QueueSize:       16,
		WorkerCount:     1,
		ShutdownTimeout: 10 * time.Second,
		Addr:            "127.0.0.1:0",
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + a.Addr()
	waitReady(t, client, base)

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The listener is gone, so probe the handler directly.
	if code := probe(a, "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d after shutdown, want 503", code)
	}
	if code := probe(a, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d after shutdown, want 200", code)
	}
}

func probe(a *App, path string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	a.api.Handler().ServeHTTP(rec, req)
	_, _ = io.Copy(io.Discard, rec.Body)
	return rec.Code
}
