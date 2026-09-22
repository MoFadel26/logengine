package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MoFadel26/logengine/internal/config"
	"github.com/MoFadel26/logengine/internal/pgtest"
)

// TestShutdownUnderLoadLosesNoAcceptedLog blasts the running service with
// concurrent ingest requests, cancels the context mid-flight (what SIGTERM
// does in main) and then proves that every record that received a 202 is in
// Postgres exactly once.
//
// The test is built so it fails if a record is lost:
//   - the expected set is built only from 202 responses, so a record dropped
//     during the drain leaves a hole;
//   - requests that failed at the transport level have an unknown outcome and
//     are tracked separately, so they can neither mask a loss nor cause a
//     false failure;
//   - it asserts that the database was still behind the accepted set when the
//     shutdown started, which means records really did have to survive the
//     drain rather than having already been flushed.
func TestShutdownUnderLoadLosesNoAcceptedLog(t *testing.T) {
	dsn := pgtest.Start(t)

	cfg := config.Config{
		DatabaseURL: dsn,
		// A small queue relative to the offered load keeps records buffered,
		// so the drain has real work to do.
		QueueSize:       256,
		WorkerCount:     4,
		ShutdownTimeout: 20 * time.Second,
		Addr:            "127.0.0.1:0",
	}

	a, err := New(context.Background(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	base := "http://" + a.Addr()
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 64},
	}
	waitReady(t, client, base)

	tenant := uuid.New()
	var (
		mu       sync.Mutex
		accepted = map[string]bool{}
		unknown  = map[string]bool{}
		rejected int
	)

	const clients = 16
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				msg := fmt.Sprintf("c%02d-i%07d", c, i)
				status, err := postLog(client, base, tenant.String(), msg)
				mu.Lock()
				switch {
				case err != nil:
					// The response never arrived: the record may or may not
					// have been enqueued before the listener closed.
					unknown[msg] = true
				case status == http.StatusAccepted:
					accepted[msg] = true
				default:
					rejected++
				}
				mu.Unlock()
				if err != nil {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(c)
	}

	// Let the load build up until enough records have been accepted.
	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		n := len(accepted)
		mu.Unlock()
		if n >= 2000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d records accepted before the deadline", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	verify, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("verification pool: %v", err)
	}
	defer verify.Close()

	depthAtCancel := a.q.Len()
	writtenAtCancel := countRows(t, verify, tenant)

	// This is the SIGTERM moment.
	cancel()

	if err := <-runErr; err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	close(stop)
	wg.Wait()

	mu.Lock()
	acceptedSet := accepted
	unknownSet := unknown
	rejectedCount := rejected
	mu.Unlock()

	stored, total := storedMessages(t, verify, tenant)
	t.Logf("accepted=%d unknown=%d rejected(503)=%d stored=%d queue_depth_at_cancel=%d rows_written_at_cancel=%d",
		len(acceptedSet), len(unknownSet), rejectedCount, len(stored), depthAtCancel, writtenAtCancel)

	if len(acceptedSet) < 1000 {
		t.Fatalf("only %d records accepted: the test did not apply meaningful load", len(acceptedSet))
	}
	if writtenAtCancel >= len(acceptedSet) {
		t.Fatalf("%d rows were already written when shutdown started but only %d records were accepted: "+
			"nothing was left to drain, so this run proves nothing",
			writtenAtCancel, len(acceptedSet))
	}

	var missing []string
	for msg := range acceptedSet {
		if !stored[msg] {
			missing = append(missing, msg)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d of %d accepted records are missing from the database (e.g. %s)",
			len(missing), len(acceptedSet), strings.Join(sample(missing, 5), ", "))
	}

	for msg := range stored {
		if !acceptedSet[msg] && !unknownSet[msg] {
			t.Errorf("record %q is stored but was never accepted", msg)
			break
		}
	}
	if total != len(stored) {
		t.Errorf("database holds %d rows but only %d distinct messages: records were duplicated", total, len(stored))
	}
}

func postLog(client *http.Client, base, tenant, msg string) (int, error) {
	body := fmt.Sprintf(`{"tenant_id":%q,"ts":%q,"level":"info","source":"load","message":%q,"attrs":{"k":"v"}}`,
		tenant, time.Now().UTC().Format(time.RFC3339Nano), msg)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/logs", strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func waitReady(t *testing.T, client *http.Client, base string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("service never became ready")
}

func countRows(t *testing.T, pool *pgxpool.Pool, tenant uuid.UUID) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM logs WHERE tenant_id = $1`, tenant).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func storedMessages(t *testing.T, pool *pgxpool.Pool, tenant uuid.UUID) (map[string]bool, int) {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT message FROM logs WHERE tenant_id = $1`, tenant)
	if err != nil {
		t.Fatalf("select messages: %v", err)
	}
	defer rows.Close()

	stored := map[string]bool{}
	total := 0
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			t.Fatalf("scan: %v", err)
		}
		stored[msg] = true
		total++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return stored, total
}

func sample(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}
