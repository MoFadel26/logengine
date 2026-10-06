// Package httpapi exposes the ingest endpoint, search API, web UI, health probes and metrics.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/pgstore"
	"github.com/MoFadel26/logengine/internal/queue"
	"github.com/MoFadel26/logengine/internal/ui"
)

// maxBodyBytes caps a single ingest request.
const maxBodyBytes = 4 << 20

// Querier executes search queries against persisted logs.
type Querier interface {
	Query(ctx context.Context, p pgstore.QueryParams) ([]logrec.Record, error)
}

// API serves the HTTP surface of the service.
type API struct {
	q       *queue.Queue
	m       *metrics.Metrics
	reg     *prometheus.Registry
	log     *slog.Logger
	querier Querier
	ready   atomic.Bool
}

// New creates the API. It starts out not ready.
func New(q *queue.Queue, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger, querier Querier) *API {
	return &API{q: q, m: m, reg: reg, log: log, querier: querier}
}

// SetReady flips the readiness probe.
func (a *API) SetReady(ready bool) { a.ready.Store(ready) }

// Handler builds the router.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", a.postLogs)
	mux.HandleFunc("GET /v1/logs", a.getLogs)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.reg, promhttp.HandlerOpts{}))

	// Embedded Web UI
	mux.Handle("GET /ui/", http.StripPrefix("/ui", ui.Handler()))
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})

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

type logResponseItem struct {
	TenantID uuid.UUID       `json:"tenant_id"`
	TS       string          `json:"ts"`
	Level    string          `json:"level,omitempty"`
	Source   string          `json:"source,omitempty"`
	Message  string          `json:"message,omitempty"`
	Attrs    json.RawMessage `json:"attrs,omitempty"`
}

type logsResponse struct {
	Count int               `json:"count"`
	Logs  []logResponseItem `json:"logs"`
}

func parseTimeParam(val string) (time.Time, error) {
	val = strings.TrimSpace(val)
	if val == "" || val == "all" {
		return time.Time{}, nil
	}

	// Check relative duration e.g. "15m", "1h", "24h", "7d", "30d"
	if strings.HasSuffix(val, "d") {
		days, err := strconv.Atoi(val[:len(val)-1])
		if err == nil && days > 0 {
			return time.Now().UTC().AddDate(0, 0, -days), nil
		}
	}
	if d, err := time.ParseDuration(val); err == nil {
		return time.Now().UTC().Add(-d), nil
	}

	// Check standard timestamp formats
	if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, val); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, errors.New("invalid time format, expected RFC3339 or duration like 15m, 1h, 7d")
}

func (a *API) getLogs(w http.ResponseWriter, r *http.Request) {
	if a.querier == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "querying logs is not configured"})
		return
	}

	q := r.URL.Query()
	var params pgstore.QueryParams

	if tenantStr := strings.TrimSpace(q.Get("tenant_id")); tenantStr != "" {
		id, err := uuid.Parse(tenantStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid tenant_id: must be a valid UUID"})
			return
		}
		params.TenantID = &id
	}

	params.Level = strings.TrimSpace(q.Get("level"))
	params.Source = strings.TrimSpace(q.Get("source"))
	params.Search = strings.TrimSpace(q.Get("q"))

	if fromStr := q.Get("from"); fromStr != "" {
		from, err := parseTimeParam(fromStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid 'from' parameter: " + err.Error()})
			return
		}
		params.From = from
	}

	if toStr := q.Get("to"); toStr != "" {
		to, err := parseTimeParam(toStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid 'to' parameter: " + err.Error()})
			return
		}
		params.To = to
	}

	if limitStr := q.Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
			params.Limit = limit
		}
	}

	recs, err := a.querier.Query(r.Context(), params)
	if err != nil {
		a.log.Error("query logs error", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to query logs"})
		return
	}

	items := make([]logResponseItem, 0, len(recs))
	for _, r := range recs {
		var raw json.RawMessage
		if len(r.Attrs) > 0 {
			raw = json.RawMessage(r.Attrs)
		}
		items = append(items, logResponseItem{
			TenantID: r.TenantID,
			TS:       r.TS.Format(time.RFC3339Nano),
			Level:    r.Level,
			Source:   r.Source,
			Message:  r.Message,
			Attrs:    raw,
		})
	}

	writeJSON(w, http.StatusOK, logsResponse{
		Count: len(items),
		Logs:  items,
	})
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
