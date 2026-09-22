// Package worker implements the pool that drains the ingest queue and writes
// batches of log records to storage.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
)

// BatchWriter persists a batch of records.
type BatchWriter interface {
	WriteBatch(ctx context.Context, recs []logrec.Record) error
}

// Config tunes the pool.
type Config struct {
	Workers       int
	BatchSize     int
	FlushInterval time.Duration
	WriteTimeout  time.Duration
}

// Pool consumes records from in until it is closed, batching them by size or
// by elapsed time, whichever comes first.
type Pool struct {
	in  <-chan logrec.Record
	w   BatchWriter
	cfg Config
	m   *metrics.Metrics
	log *slog.Logger
	wg  sync.WaitGroup
}

// New creates a pool reading from in.
func New(in <-chan logrec.Record, w BatchWriter, cfg Config, m *metrics.Metrics, log *slog.Logger) *Pool {
	return &Pool{in: in, w: w, cfg: cfg, m: m, log: log}
}

// Start launches Workers goroutines.
func (p *Pool) Start() {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			p.run(id)
		}(i)
	}
}

// Wait blocks until every worker has drained the queue and flushed its final
// batch, or until ctx is done.
func (p *Pool) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) run(id int) {
	batch := make([]logrec.Record, 0, p.cfg.BatchSize)
	timer := time.NewTimer(p.cfg.FlushInterval)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(p.cfg.FlushInterval)
	}

	for {
		select {
		case rec, ok := <-p.in:
			if !ok {
				// Queue closed and drained: flush whatever is left.
				p.flush(id, batch)
				return
			}
			batch = append(batch, rec)
			if len(batch) >= p.cfg.BatchSize {
				p.flush(id, batch)
				batch = batch[:0]
				resetTimer()
			}
		case <-timer.C:
			if len(batch) > 0 {
				p.flush(id, batch)
				batch = batch[:0]
			}
			timer.Reset(p.cfg.FlushInterval)
		}
	}
}

func (p *Pool) flush(id int, batch []logrec.Record) {
	if len(batch) == 0 {
		return
	}
	// Deliberately rooted at Background: the pool must be able to drain after
	// the signal context has been cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
	defer cancel()

	start := time.Now()
	err := p.w.WriteBatch(ctx, batch)
	elapsed := time.Since(start)

	p.m.BatchLatency.Observe(elapsed.Seconds())
	p.m.BatchSize.Observe(float64(len(batch)))
	if err != nil {
		p.m.WriteErrors.Inc()
		p.log.Error("batch write failed", "worker", id, "rows", len(batch), "err", err)
		return
	}
	p.m.RowsWritten.Add(float64(len(batch)))
	p.log.Debug("batch written", "worker", id, "rows", len(batch), "duration_ms", elapsed.Milliseconds())
}
