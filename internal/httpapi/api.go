// Package httpapi exposes the ingest endpoint, health probes and metrics.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/queue"
)

// maxBodyBytes caps a single ingest request.
const maxBodyBytes = 4 << 20

// API serves the HTTP surface of the service.
type API struct {
	q     *queue.Queue
	m     *metrics.Metrics
	reg   *prometheus.Registry
	log   *slog.Logger
	ready atomic.Bool
}

// New creates the API. It starts out not ready.
func New(q *queue.Queue, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger) *API {
	return &API{q: q, m: m, reg: reg, log: log}
}

// SetReady flips the readiness probe.
func (a *API) SetReady(ready bool) { a.ready.Store(ready) }

// Handler builds the router.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", a.postLogs)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.reg, promhttp.HandlerOpts{}))
	return mux
}

func (a *API) postLogs(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	recs, err := logrec.Decode(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "request body too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	// Non-blocking sends only. Accepted records are always a prefix of the
	// submitted array, so a client that gets a partial 503 knows exactly which
	// records to resend.
	accepted := 0
	for _, rec := range recs {
		if !a.q.TryEnqueue(rec) {
			break
		}
		accepted++
	}
	a.m.Accepted.Add(float64(accepted))

	if dropped := len(recs) - accepted; dropped > 0 {
		a.m.Dropped.Add(float64(dropped))
		a.log.Warn("ingest queue full", "accepted", accepted, "dropped", dropped, "queue_depth", a.q.Len())
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":    "ingest queue full",
			"accepted": accepted,
			"dropped":  dropped,
		})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted})
}

func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (a *API) readyz(w http.ResponseWriter, _ *http.Request) {
	if !a.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "shutting down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
