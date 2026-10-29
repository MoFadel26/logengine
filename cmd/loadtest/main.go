// Command loadtest drives POST /v1/logs with batches of synthetic records and
// reports client-side latency, status codes, accepted/dropped counts and the
// rows/sec the server persisted, read from its /metrics endpoint.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	levels  = []string{"DEBUG", "INFO", "WARN", "ERROR"}
	sources = []string{"svc-api", "svc-worker", "svc-gateway", "svc-auth", "svc-billing"}
)

type record struct {
	TenantID string         `json:"tenant_id"`
	TS       string         `json:"ts"`
	Level    string         `json:"level"`
	Source   string         `json:"source"`
	Message  string         `json:"message"`
	Attrs    map[string]any `json:"attrs"`
}

// result is what one worker observed.
type result struct {
	latencies  []time.Duration
	status     map[int]int
	retryAfter int // 503 responses carrying a Retry-After header
	errors     int // transport errors
	accepted   int
	dropped    int
}

func main() {
	base := flag.String("url", "http://localhost:8080", "base URL of the API")
	duration := flag.Duration("duration", 30*time.Second, "how long to send load")
	conc := flag.Int("c", 10, "concurrent senders")
	batch := flag.Int("batch", 100, "records per request")
	rate := flag.Float64("rate", 0, "target requests/sec across all senders (0 = as fast as possible)")
	drain := flag.Duration("drain", 60*time.Second, "max wait for the server queue to drain after the run")
	flag.Parse()

	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *conc},
	}
	metricsURL := *base + "/metrics"
	before := scrape(client, metricsURL)

	var ticks <-chan time.Time
	if *rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / *rate))
		defer t.Stop()
		ticks = t.C
	}

	start := time.Now()
	deadline := start.Add(*duration)
	results := make([]result, *conc)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(r *result, worker int) {
			defer wg.Done()
			r.status = map[int]int{}
			for seq := 0; time.Now().Before(deadline); seq++ {
				if ticks != nil {
					select {
					case <-ticks:
					case <-time.After(time.Until(deadline)):
						return
					}
				}
				send(client, *base+"/v1/logs", body(*batch, worker, seq), r)
			}
		}(&results[i], i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	atEnd := scrape(client, metricsURL)

	// Drain: wait for the queue to empty and for rows_written to stop moving,
	// so batches already pulled off the queue by workers are flushed too. The
	// poll interval is longer than the server's 500ms flush interval. drained
	// is when the last increase was seen, to 1s resolution.
	drainStart := time.Now()
	after := atEnd
	drained := elapsed
	for time.Since(drainStart) < *drain {
		time.Sleep(time.Second)
		next := scrape(client, metricsURL)
		moved := next["logengine_rows_written_total"] != after["logengine_rows_written_total"]
		after = next
		if moved {
			drained = time.Since(start)
		} else if next["logengine_queue_depth"] == 0 {
			break
		}
	}

	report(results, elapsed, drained, before, atEnd, after)
}

func body(n, worker, seq int) []byte {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	recs := make([]record, n)
	for i := range recs {
		k := worker*7 + seq*13 + i
		recs[i] = record{
			TenantID: fmt.Sprintf("00000000-0000-0000-0000-%012d", 1+k%10),
			TS:       now,
			Level:    levels[k%len(levels)],
			Source:   sources[k%len(sources)],
			Message:  fmt.Sprintf("loadtest worker=%d seq=%d i=%d", worker, seq, i),
			Attrs:    map[string]any{"worker": worker, "seq": seq, "duration_ms": k % 500},
		}
	}
	b, err := json.Marshal(recs)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func send(client *http.Client, url string, payload []byte, r *result) {
	t0 := time.Now()
	resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		r.errors++
		return
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	r.latencies = append(r.latencies, time.Since(t0))
	r.status[resp.StatusCode]++

	var counts struct{ Accepted, Dropped int }
	_ = json.Unmarshal(raw, &counts)
	r.accepted += counts.Accepted
	r.dropped += counts.Dropped
	if resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get("Retry-After") != "" {
		r.retryAfter++
	}
}

// scrape reads the unlabelled logengine_* samples from a Prometheus text page.
func scrape(client *http.Client, url string) map[string]float64 {
	resp, err := client.Get(url)
	if err != nil {
		log.Fatalf("scrape %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		name, val, ok := strings.Cut(sc.Text(), " ")
		if !ok || !strings.HasPrefix(name, "logengine_") {
			continue
		}
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			out[name] = v
		}
	}
	return out
}

func report(results []result, elapsed, drained time.Duration, before, atEnd, after map[string]float64) {
	var all []time.Duration
	status := map[int]int{}
	var retryAfter, errs, accepted, dropped int
	for _, r := range results {
		all = append(all, r.latencies...)
		for code, n := range r.status {
			status[code] += n
		}
		retryAfter += r.retryAfter
		errs += r.errors
		accepted += r.accepted
		dropped += r.dropped
	}
	slices.Sort(all)
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(p*float64(len(all)-1))]
	}
	other := 0
	for code, n := range status {
		if code != http.StatusAccepted && code != http.StatusServiceUnavailable {
			other += n
		}
	}
	delta := func(m map[string]float64, name string) float64 { return m[name] - before[name] }
	written := delta(after, "logengine_rows_written_total")

	w := os.Stdout
	_, _ = fmt.Fprintf(w, "duration          %s (last rows written at %s)\n", elapsed.Round(time.Millisecond), drained.Round(time.Millisecond))
	_, _ = fmt.Fprintf(w, "requests          %d (%.0f req/s), transport errors %d\n", len(all)+errs, float64(len(all)+errs)/elapsed.Seconds(), errs)
	_, _ = fmt.Fprintf(w, "status            202=%d 503=%d other=%d (503 with Retry-After: %d)\n",
		status[http.StatusAccepted], status[http.StatusServiceUnavailable], other, retryAfter)
	_, _ = fmt.Fprintf(w, "records           accepted=%d dropped=%d\n", accepted, dropped)
	_, _ = fmt.Fprintf(w, "latency           p50=%s p95=%s p99=%s max=%s\n",
		pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(1).Round(time.Microsecond))
	_, _ = fmt.Fprintf(w, "rows/sec persisted %.0f during run, %.0f including drain\n",
		delta(atEnd, "logengine_rows_written_total")/elapsed.Seconds(), written/drained.Seconds())
	_, _ = fmt.Fprintf(w, "server deltas     accepted=%.0f rows_written=%.0f dropped=%.0f lost=%.0f write_errors=%.0f queue_depth=%.0f\n",
		delta(after, "logengine_logs_accepted_total"), written, delta(after, "logengine_logs_dropped_total"),
		delta(after, "logengine_logs_lost_total"), delta(after, "logengine_batch_write_errors_total"), after["logengine_queue_depth"])
}
