package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/queue"
)

type fakeWriter struct {
	mu         sync.Mutex
	messages   []string
	batchSizes []int
	err        error
	delay      time.Duration
}

func (f *fakeWriter) WriteBatch(_ context.Context, recs []logrec.Record) error {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.batchSizes = append(f.batchSizes, len(recs))
	for _, r := range recs {
		f.messages = append(f.messages, r.Message)
	}
	return nil
}

func (f *fakeWriter) snapshot() ([]string, []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.messages...), append([]int(nil), f.batchSizes...)
}

func newPool(t *testing.T, q *queue.Queue, w BatchWriter, cfg Config) *Pool {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	p := New(q.C(), w, cfg, m, slog.New(slog.DiscardHandler))
	p.Start()
	return p
}

func rec(msg string) logrec.Record { return logrec.Record{Message: msg, TS: time.Now()} }

func TestFlushesOnBatchSize(t *testing.T) {
	q := queue.New(64)
	w := &fakeWriter{}
	// A flush interval far longer than the test: only the size trigger can fire.
	p := newPool(t, q, w, Config{Workers: 1, BatchSize: 3, FlushInterval: time.Hour, WriteTimeout: time.Second})

	for i := 0; i < 3; i++ {
		if !q.TryEnqueue(rec(fmt.Sprintf("m%d", i))) {
			t.Fatal("enqueue failed")
		}
	}

	waitFor(t, time.Second, func() bool {
		msgs, _ := w.snapshot()
		return len(msgs) == 3
	}, "batch was not flushed after reaching the size trigger")

	_, sizes := w.snapshot()
	if len(sizes) != 1 || sizes[0] != 3 {
		t.Errorf("batch sizes = %v, want one batch of 3", sizes)
	}

	q.Close()
	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestFlushesOnInterval(t *testing.T) {
	q := queue.New(64)
	w := &fakeWriter{}
	// A batch size far larger than what we send: only the time trigger can fire.
	p := newPool(t, q, w, Config{Workers: 1, BatchSize: 10000, FlushInterval: 50 * time.Millisecond, WriteTimeout: time.Second})

	q.TryEnqueue(rec("solo"))

	waitFor(t, time.Second, func() bool {
		msgs, _ := w.snapshot()
		return len(msgs) == 1
	}, "partial batch was not flushed by the interval trigger")

	q.Close()
	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestFinalFlushOnQueueClose(t *testing.T) {
	q := queue.New(64)
	w := &fakeWriter{}
	// Neither trigger can fire before Close: the records can only reach the
	// writer through the final flush.
	p := newPool(t, q, w, Config{Workers: 1, BatchSize: 10000, FlushInterval: time.Hour, WriteTimeout: time.Second})

	for i := 0; i < 5; i++ {
		q.TryEnqueue(rec(fmt.Sprintf("m%d", i)))
	}
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	msgs, _ := w.snapshot()
	if len(msgs) != 5 {
		t.Fatalf("wrote %d records, want 5: the final partial batch was lost", len(msgs))
	}
}

func TestDrainsEveryRecordExactlyOnce(t *testing.T) {
	const total = 5000
	q := queue.New(total)
	w := &fakeWriter{}
	p := newPool(t, q, w, Config{Workers: 8, BatchSize: 64, FlushInterval: 10 * time.Millisecond, WriteTimeout: time.Second})

	var produced sync.WaitGroup
	for g := 0; g < 10; g++ {
		produced.Add(1)
		go func(g int) {
			defer produced.Done()
			for i := 0; i < total/10; i++ {
				for !q.TryEnqueue(rec(fmt.Sprintf("g%d-i%d", g, i))) {
					time.Sleep(time.Millisecond)
				}
			}
		}(g)
	}
	produced.Wait()
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	msgs, _ := w.snapshot()
	if len(msgs) != total {
		t.Fatalf("wrote %d records, want %d", len(msgs), total)
	}
	seen := make(map[string]int, total)
	for _, m := range msgs {
		seen[m]++
	}
	if len(seen) != total {
		t.Fatalf("%d distinct records written, want %d (duplicates or losses)", len(seen), total)
	}
}

func TestWaitRespectsContextDeadline(t *testing.T) {
	q := queue.New(8)
	w := &fakeWriter{delay: 500 * time.Millisecond}
	p := newPool(t, q, w, Config{Workers: 1, BatchSize: 1, FlushInterval: time.Hour, WriteTimeout: time.Second})
	q.TryEnqueue(rec("slow"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait error = %v, want context.DeadlineExceeded", err)
	}

	q.Close()
	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after close: %v", err)
	}
}

func TestWriteErrorIsCountedAndDoesNotStall(t *testing.T) {
	q := queue.New(8)
	w := &fakeWriter{err: errors.New("boom")}
	m := metrics.New(prometheus.NewRegistry())
	p := New(q.C(), w, Config{Workers: 1, BatchSize: 2, FlushInterval: time.Hour, WriteTimeout: time.Second}, m, slog.New(slog.DiscardHandler))
	p.Start()

	q.TryEnqueue(rec("a"))
	q.TryEnqueue(rec("b"))
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}
