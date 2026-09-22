package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MoFadel26/logengine/internal/metrics"
	"github.com/MoFadel26/logengine/internal/queue"
)

const tenant = "3f0c1b2a-8d4e-4b6f-9a1c-2e5d7f8a9b0c"

func newAPI(t *testing.T, queueSize int) (*API, *queue.Queue) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	q := queue.New(queueSize)
	m.RegisterQueueDepth(q.Len)
	api := New(q, m, reg, slog.New(slog.DiscardHandler))
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

	body := "[" + record("a") + "," + record("b") + "," + record("c") + "]"
	rec := post(t, api, body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body)
	}
	if got := decodeCount(t, rec, "accepted"); got != 3 {
		t.Errorf("accepted = %d, want 3", got)
	}
	if q.Len() != 3 {
		t.Errorf("queue depth = %d, want 3", q.Len())
	}
}

func TestPostInvalidPayloadRejected(t *testing.T) {
	api, q := newAPI(t, 4)

	for _, body := range []string{
		`{"tenant_id":"not-a-uuid","ts":"2026-09-22T10:00:00Z"}`,
		`{"ts":"2026-09-22T10:00:00Z"}`,
		`[` + record("ok") + `,{"tenant_id":"bad","ts":"2026-09-22T10:00:00Z"}]`,
		`{`,
	} {
		rec := post(t, api, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 for %s", rec.Code, body)
		}
	}
	if q.Len() != 0 {
		t.Errorf("queue depth = %d, want 0: rejected payloads must not be enqueued", q.Len())
	}
}

func TestBackpressureReturns503WithRetryAfter(t *testing.T) {
	api, q := newAPI(t, 2)

	for i := 0; i < 2; i++ {
		if rec := post(t, api, record(fmt.Sprintf("fill-%d", i))); rec.Code != http.StatusAccepted {
			t.Fatalf("fill %d: status = %d, want 202", i, rec.Code)
		}
	}

	rec := post(t, api, record("overflow"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 once the queue is full", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("Retry-After header missing on 503")
	}
	if got := decodeCount(t, rec, "dropped"); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
	if q.Len() != 2 {
		t.Errorf("queue depth = %d, want 2: the handler must not have blocked or overfilled", q.Len())
	}
	assertCounter(t, api, "logengine_logs_dropped_total", 1)
	assertCounter(t, api, "logengine_logs_accepted_total", 2)
}

func TestBackpressurePartialArrayReportsAcceptedPrefix(t *testing.T) {
	api, q := newAPI(t, 2)

	body := "[" + record("a") + "," + record("b") + "," + record("c") + "," + record("d") + "]"
	rec := post(t, api, body)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := decodeCount(t, rec, "accepted"); got != 2 {
		t.Errorf("accepted = %d, want 2", got)
	}
	if got := decodeCount(t, rec, "dropped"); got != 2 {
		t.Errorf("dropped = %d, want 2", got)
	}
	// The accepted records must be the first two, so the client can resend the rest.
	for _, want := range []string{"a", "b"} {
		if got := <-q.C(); got.Message != want {
			t.Errorf("queued message = %q, want %q", got.Message, want)
		}
	}
}

func TestEnqueueFailsAfterQueueClosed(t *testing.T) {
	api, q := newAPI(t, 4)
	q.Close()

	rec := post(t, api, record("after close"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after the queue is closed", rec.Code)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	api, _ := newAPI(t, 4)

	for _, path := range []string{"/healthz", "/readyz"} {
		if code := get(api, path); code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, code)
		}
	}

	api.SetReady(false)
	if code := get(api, "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz = %d, want 503 once readiness is off", code)
	}
	if code := get(api, "/healthz"); code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200: liveness must stay up during drain", code)
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
	req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/logs = %d, want 405", rec.Code)
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
