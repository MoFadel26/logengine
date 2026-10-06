package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/pgstore"
	"github.com/MoFadel26/logengine/internal/queue"
)

const tenant = "3f0c1b2a-8d4e-4b6f-9a1c-2e5d7f8a9b0c"

type fakeQuerier struct {
	recs []logrec.Record
	err  error
}

func (f *fakeQuerier) Query(_ context.Context, _ pgstore.QueryParams) ([]logrec.Record, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.recs, nil
}

func newAPI(t *testing.T, queueSize int) (*API, *queue.Queue) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(queueSize)
	m.RegisterQueueDepth(q.Len)
	api := New(q, m, reg, slog.New(slog.DiscardHandler), nil)
	api.SetReady(true)
	return api, q
}

func post(t *testing.T, api *API, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	return rec
}

func record(msg string) string {
	return fmt.Sprintf(`{"tenant_id":%q,"ts":"2026-09-22T10:00:00Z","level":"info","source":"api","message":%q}`, tenant, msg)
}

func TestPostSingleLogAccepted(t *testing.T) {
	api, q := newAPI(t, 4)

	rec := post(t, api, record("hello"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body)
	}
	if got := decodeCount(t, rec, "accepted"); got != 1 {
		t.Errorf("accepted = %d, want 1", got)
	}
	if q.Len() != 1 {
		t.Errorf("queue depth = %d, want 1", q.Len())
	}
	got := <-q.C()
	if got.Message != "hello" {
		t.Errorf("queued message = %q, want %q", got.Message, "hello")
	}
}

func TestPostArrayAccepted(t *testing.T) {
	api, q := newAPI(t, 4)

	rec := post(t, api, fmt.Sprintf("[%s, %s]", record("one"), record("two")))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body)
	}
	if got := decodeCount(t, rec, "accepted"); got != 2 {
		t.Errorf("accepted = %d, want 2", got)
	}
	if q.Len() != 2 {
		t.Errorf("queue depth = %d, want 2", q.Len())
	}
}

func TestQueueFullReturns503WithRetryAfter(t *testing.T) {
	api, _ := newAPI(t, 1)

	// Fill the queue.
	if rec := post(t, api, record("fill")); rec.Code != http.StatusAccepted {
		t.Fatalf("fill request failed: %d", rec.Code)
	}

	// Next submit must get 503.
	rec := post(t, api, record("blocked"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if h := rec.Header().Get("Retry-After"); h != "1" {
		t.Errorf("Retry-After = %q, want \"1\"", h)
	}
	if got := decodeCount(t, rec, "accepted"); got != 0 {
		t.Errorf("accepted = %d, want 0", got)
	}
	if got := decodeCount(t, rec, "dropped"); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
}

func TestPartialEnqueueReflectsPrefixAccepted(t *testing.T) {
	api, _ := newAPI(t, 2)

	// Submit three records when only two fit.
	rec := post(t, api, fmt.Sprintf("[%s, %s, %s]", record("a"), record("b"), record("c")))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := decodeCount(t, rec, "accepted"); got != 2 {
		t.Errorf("accepted = %d, want 2", got)
	}
	if got := decodeCount(t, rec, "dropped"); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
	assertCounter(t, api, "logengine_logs_accepted_total", 2)
	assertCounter(t, api, "logengine_logs_dropped_total", 1)
}

func TestInvalidPayloadRejectedWith400(t *testing.T) {
	api, _ := newAPI(t, 4)

	for name, body := range map[string]string{
		"not_json":        "not json",
		"empty_body":      "",
		"empty_array":     "[]",
		"missing_tenant":  `{"ts":"2026-09-22T10:00:00Z","message":"x"}`,
		"bad_uuid":        `{"tenant_id":"bad","ts":"2026-09-22T10:00:00Z"}`,
		"missing_ts":      `{"tenant_id":"` + tenant + `"}`,
		"bad_ts":          `{"tenant_id":"` + tenant + `","ts":"yesterday"}`,
		"attrs_not_obj":   `{"tenant_id":"` + tenant + `","ts":"2026-09-22T10:00:00Z","attrs":[1,2]}`,
		"array_one_bad":   fmt.Sprintf("[%s, {\"bad\":true}]", record("ok")),
	} {
		t.Run(name, func(t *testing.T) {
			rec := post(t, api, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
			}
		})
	}
}

func TestHealthProbes(t *testing.T) {
	api, _ := newAPI(t, 4)

	if got := get(api, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", got)
	}
	if got := get(api, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", got)
	}

	api.SetReady(false)
	if got := get(api, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz after unready = %d, want 200", got)
	}
	if got := get(api, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz after unready = %d, want 503", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	api, _ := newAPI(t, 4)
	post(t, api, record("metric"))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	for _, name := range []string{
		"logengine_queue_depth",
		"logengine_logs_dropped_total",
		"logengine_batch_write_duration_seconds",
	} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Errorf("metric %q missing from /metrics", name)
		}
	}
	if !strings.Contains(rec.Body.String(), "logengine_queue_depth 1") {
		t.Error("queue depth gauge does not reflect the buffered record")
	}
}

func TestWrongMethod(t *testing.T) {
	api, _ := newAPI(t, 4)
	req := httptest.NewRequest(http.MethodPut, "/v1/logs", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /v1/logs = %d, want 405", rec.Code)
	}
}

func TestOversizedBodyRejected(t *testing.T) {
	api, _ := newAPI(t, 4)
	huge := strings.Repeat("x", maxBodyBytes+1)
	rec := post(t, api, `{"tenant_id":"`+tenant+`","ts":"2026-09-22T10:00:00Z","message":"`+huge+`"}`)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestGetLogsSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(4)

	tenantUUID := uuid.MustParse(tenant)
	now := time.Now().UTC()
	querier := &fakeQuerier{
		recs: []logrec.Record{
			{
				TenantID: tenantUUID,
				TS:       now,
				Level:    "INFO",
				Source:   "api",
				Message:  "test log",
				Attrs:    []byte(`{"foo":"bar"}`),
			},
		},
	}

	api := New(q, m, reg, slog.New(slog.DiscardHandler), querier)
	api.SetReady(true)

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?tenant_id="+tenant+"&level=INFO&from=1h&limit=50", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/logs = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}

	var resp logsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Count != 1 || len(resp.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", resp.Count)
	}
	if resp.Logs[0].Message != "test log" {
		t.Errorf("expected message 'test log', got %q", resp.Logs[0].Message)
	}
	if string(resp.Logs[0].Attrs) != `{"foo":"bar"}` {
		t.Errorf("expected attrs '{\"foo\":\"bar\"}', got %s", string(resp.Logs[0].Attrs))
	}
}

func TestGetLogsValidationErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(4)
	querier := &fakeQuerier{}
	api := New(q, m, reg, slog.New(slog.DiscardHandler), querier)

	// Invalid UUID
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?tenant_id=invalid-uuid", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad tenant_id, got %d", rec.Code)
	}

	// Invalid From
	req = httptest.NewRequest(http.MethodGet, "/v1/logs?from=not-a-date", nil)
	rec = httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad from param, got %d", rec.Code)
	}
}

func TestGetLogsQuerierError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(4)
	querier := &fakeQuerier{err: errors.New("db down")}
	api := New(q, m, reg, slog.New(slog.DiscardHandler), querier)

	req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 when querier errors, got %d", rec.Code)
	}
}

func TestUIEndpoints(t *testing.T) {
	api, _ := newAPI(t, 4)

	// GET / redirects to /ui/
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Errorf("GET / = %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/ui/" {
		t.Errorf("GET / Location = %q, want /ui/", loc)
	}

	// GET /ui/ serves index.html
	req = httptest.NewRequest(http.MethodGet, "/ui/", nil)
	rec = httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /ui/ = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "logengine") {
		t.Errorf("GET /ui/ does not contain 'logengine'")
	}
}

func get(api *API, path string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	return rec.Code
}

func decodeCount(t *testing.T, rec *httptest.ResponseRecorder, key string) int {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response %s: %v", rec.Body.String(), err)
	}
	v, ok := body[key].(float64)
	if !ok {
		t.Fatalf("response %s has no numeric %q", rec.Body.String(), key)
	}
	return int(v)
}

func assertCounter(t *testing.T, api *API, name string, want float64) {
	t.Helper()
	families, err := api.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		if got := f.GetMetric()[0].GetCounter().GetValue(); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
		return
	}
	t.Errorf("counter %q not found", name)
}
