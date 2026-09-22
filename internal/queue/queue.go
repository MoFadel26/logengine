// Package queue implements the bounded, non-blocking ingest buffer that sits
// between the HTTP handler and the worker pool.
package queue

import (
	"sync"

	"github.com/MoFadel26/logengine/internal/logrec"
)

// Queue is a buffered channel of log records. Producers never block: a send
// into a full (or closed) queue fails immediately so the HTTP handler can
// answer with 503.
type Queue struct {
	ch chan logrec.Record

	mu     sync.RWMutex
	closed bool
}

// New creates a queue buffering up to size records.
func New(size int) *Queue {
	return &Queue{ch: make(chan logrec.Record, size)}
}

// TryEnqueue performs a non-blocking send. It reports whether the record was
// buffered.
func (q *Queue) TryEnqueue(rec logrec.Record) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return false
	}
	select {
	case q.ch <- rec:
		return true
	default:
		return false
	}
}

// C returns the receive side of the queue for the worker pool.
func (q *Queue) C() <-chan logrec.Record { return q.ch }

// Close closes the queue so workers drain the remaining records and stop.
// Further TryEnqueue calls fail instead of panicking on a closed channel.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.ch)
}

// Len is the number of records currently buffered.
func (q *Queue) Len() int { return len(q.ch) }
