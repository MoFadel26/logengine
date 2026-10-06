// Package app wires the ingest pipeline together and owns its lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/MoFadel26/logengine/internal/config"
	"github.com/MoFadel26/logengine/internal/httpapi"
	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/pgstore"
	"github.com/MoFadel26/logengine/internal/queue"
	"github.com/MoFadel26/logengine/internal/worker"
)

const (
	batchSize         = 500
	flushInterval     = 500 * time.Millisecond
	writeTimeout      = 10 * time.Second
	readHeaderTimeout = 10 * time.Second
	retryInitial      = 100 * time.Millisecond
	retryMax          = 5 * time.Second
)

// App is the running service.
type App struct {
	cfg  config.Config
	log  *slog.Logger
	pool *pgxpool.Pool
	q    *queue.Queue
	wp   *worker.Pool
	api  *httpapi.API
	srv  *http.Server
	ln   net.Listener
}

// New connects to Postgres and binds the listener. The service is not serving
// until Run is called.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(cfg.QueueSize)
	m.RegisterQueueDepth(q.Len)

	store := pgstore.New(pool)
	wp := worker.New(q.C(), store, worker.Config{
		Workers:       cfg.WorkerCount,
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
		WriteTimeout:  writeTimeout,
		RetryInitial:  retryInitial,
		RetryMax:      retryMax,
	}, m, log)

	api := httpapi.New(q, m, reg, log, store)

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	return &App{
		cfg:  cfg,
		log:  log,
		pool: pool,
		q:    q,
		wp:   wp,
		api:  api,
		srv: &http.Server{
			Handler:           api.Handler(),
			ReadHeaderTimeout: readHeaderTimeout,
		},
		ln: ln,
	}, nil
}

// Addr is the address the service is listening on.
func (a *App) Addr() string { return a.ln.Addr().String() }

// Run serves until ctx is cancelled, then shuts down gracefully in this order:
// readiness fails, HTTP server drains, ingest queue closes, workers flush what
// is left (bounded by SHUTDOWN_TIMEOUT), database pool closes.
func (a *App) Run(ctx context.Context) error {
	a.wp.Start()
	a.api.SetReady(true)
	a.log.Info("serving", "addr", a.Addr(), "workers", a.cfg.WorkerCount, "queue_size", a.cfg.QueueSize)

	serveErr := make(chan error, 1)
	go func() {
		err := a.srv.Serve(a.ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		// The server stopped on its own; still drain the pipeline.
		a.shutdown()
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
		a.log.Info("shutdown signal received", "timeout", a.cfg.ShutdownTimeout)
		err := a.shutdown()
		<-serveErr
		return err
	}
}

func (a *App) shutdown() error {
	// The shutdown deadline is independent of the (already cancelled) signal
	// context.
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	// 1. Fail readiness so load balancers stop sending new traffic.
	a.api.SetReady(false)

	// 2. Stop accepting requests and let in-flight handlers finish. Once this
	// returns, no handler can still be enqueuing, which makes closing the
	// queue safe.
	var err error
	if shutErr := a.srv.Shutdown(ctx); shutErr != nil {
		err = fmt.Errorf("http shutdown: %w", shutErr)
		a.log.Error("http shutdown did not complete", "err", shutErr)
	}

	// 3. Close the queue so workers drain and flush their final batches.
	a.q.Close()

	// 4. Wait for the workers, bounded by SHUTDOWN_TIMEOUT. Failed batches are
	// retried with backoff inside that budget; anything still unwritten when it
	// runs out is abandoned and reported here, which makes the process exit
	// non-zero rather than silently losing accepted records.
	if waitErr := a.wp.Wait(ctx); waitErr != nil {
		a.log.Error("worker pool did not drain in time", "err", waitErr, "queue_depth", a.q.Len())
		if err == nil {
			err = fmt.Errorf("drain ingest queue: %w", waitErr)
		}
	}
	if lost := a.wp.Lost(); lost > 0 {
		a.log.Error("accepted records lost at shutdown", "lost", lost)
	}
	a.wp.Close()

	// 5. Release the database pool.
	a.pool.Close()
	a.log.Info("shutdown complete")
	return err
}
