// Package worker implements the pool that drains the ingest queue and writes
// batches of log records to storage.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
)

// abandonGrace is how long Wait gives workers to abandon their in-flight
// batches once retries have been called off, so the lost count it reports is
// complete.
const abandonGrace = time.Second

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
	RetryInitial  time.Duration
	RetryMax      time.Duration
}

// Pool consumes records from in until it is closed, batching them by size or
// by elapsed time, whichever comes first.
//
// A failed write is retried with exponential backoff rather than dropped, so a
// transient database outage costs latency instead of data. Retries continue
// until they succeed or until the shutdown deadline calls them off; whatever
// is still unwritten at that point is counted as lost and reported by Wait.
type Pool struct {
	in  <-chan logrec.Record
	w   BatchWriter
	cfg Config
	m   *metrics.Metrics
	log *slog.Logger
	wg  sync.WaitGroup

	// retryCtx is cancelled when the shutdown deadline passes, which is what
	// stops workers retrying forever and lets them exit.
	retryCtx    context.Context
	cancelRetry context.CancelFunc

	lost atomic.Int64
}

// New creates a pool reading from in.
func New(in <-chan logrec.Record, w BatchWriter, cfg Config, m *metrics.Metrics, log *slog.Logger) *Pool {
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = 50 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 5 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{in: in, w: w, cfg: cfg, m: m, log: log, retryCtx: ctx, cancelRetry: cancel}
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

// Lost is the number of accepted records the pool failed to persist.
func (p *Pool) Lost() int64 { return p.lost.Load() }

// Wait blocks until every worker has drained the queue and flushed its final
// batch, or until ctx is done. When ctx expires it calls off outstanding
// retries so workers can exit, and reports anything that went unwritten.
func (p *Pool) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		// Out of time: stop retrying, then give workers a moment to abandon
		// their batches so the lost count below is complete.
		p.cancelRetry()
		select {
		case <-done:
		case <-time.After(abandonGrace):
		}
		err = ctx.Err()
	}

	if lost := p.Lost(); lost > 0 {
		lostErr := fmt.Errorf("%d accepted records could not be written", lost)
		if err != nil {
			err = errors.Join(err, lostErr)
		} else {
			err = lostErr
		}
	}
	return err
}

// Close releases the pool's retry context. It is safe to call more than once.
func (p *Pool) Close() { p.cancelRetry() }

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
	p.m.BatchSize.Observe(float64(len(batch)))

	backoff := p.cfg.RetryInitial
	for attempt := 1; ; attempt++ {
		// Deliberately rooted at Background: the pool must be able to drain
		// after the signal context has been cancelled.
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.WriteTimeout)
		start := time.Now()
		err := p.w.WriteBatch(ctx, batch)
		cancel()
		p.m.BatchLatency.Observe(time.Since(start).Seconds())

		if err == nil {
			p.m.RowsWritten.Add(float64(len(batch)))
			if attempt > 1 {
				p.log.Info("batch written after retry", "worker", id, "rows", len(batch), "attempts", attempt)
			}
			return
		}

		p.m.WriteErrors.Inc()
		p.log.Error("batch write failed", "worker", id, "rows", len(batch), "attempt", attempt, "err", err)

		if !p.waitBeforeRetry(backoff) {
			p.lost.Add(int64(len(batch)))
			p.m.Lost.Add(float64(len(batch)))
			p.log.Error("batch abandoned, accepted records lost",
				"worker", id, "rows", len(batch), "attempts", attempt, "err", err)
			return
		}
		if backoff *= 2; backoff > p.cfg.RetryMax {
			backoff = p.cfg.RetryMax
		}
	}
}

// waitBeforeRetry sleeps for d, reporting false if retries have been called off
// because the shutdown deadline passed.
func (p *Pool) waitBeforeRetry(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-p.retryCtx.Done():
		return false
	}
}
