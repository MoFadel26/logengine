package pgstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MoFadel26/logengine/internal/logrec"
	"github.com/MoFadel26/logengine/internal/pgstore"
	"github.com/MoFadel26/logengine/internal/pgtest"
)

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestWriteBatchRoundTrip(t *testing.T) {
	dsn := pgtest.Start(t)
	pool := newPool(t, dsn)
	w := pgstore.New(pool)

	tenant := uuid.New()
	ts := time.Now().UTC().Truncate(time.Microsecond)
	recs := []logrec.Record{
		{TenantID: tenant, TS: ts, Level: "info", Source: "api", Message: "with attrs", Attrs: []byte(`{"region":"eu-west-1","retries":3}`)},
		{TenantID: tenant, TS: ts.Add(time.Millisecond), Level: "error", Source: "worker", Message: "without attrs"},
	}

	if err := w.WriteBatch(context.Background(), recs); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	rows, err := pool.Query(context.Background(),
		`SELECT id, ts, level, source, message, attrs->>'region', attrs->>'retries'
		 FROM logs WHERE tenant_id = $1 ORDER BY ts`, tenant)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		id      int64
		ts      time.Time
		level   string
		source  string
		message string
		region  *string
		retries *string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.ts, &r.level, &r.source, &r.message, &r.region, &r.retries); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if got[0].message != "with attrs" || got[1].message != "without attrs" {
		t.Errorf("unexpected messages: %q, %q", got[0].message, got[1].message)
	}
	if got[0].level != "info" || got[0].source != "api" {
		t.Errorf("unexpected level/source: %q/%q", got[0].level, got[0].source)
	}
	if !got[0].ts.Equal(ts) {
		t.Errorf("ts = %v, want %v", got[0].ts, ts)
	}
	if got[0].region == nil || *got[0].region != "eu-west-1" {
		t.Errorf("attrs->>'region' = %v, want eu-west-1: jsonb did not round-trip", got[0].region)
	}
	if got[0].retries == nil || *got[0].retries != "3" {
		t.Errorf("attrs->>'retries' = %v, want 3", got[0].retries)
	}
	if got[1].region != nil {
		t.Errorf("absent attrs stored as %v, want NULL", *got[1].region)
	}
	// id is GENERATED ALWAYS AS IDENTITY: the database, not the writer, fills it.
	if got[0].id == 0 || got[0].id == got[1].id {
		t.Errorf("generated ids = %d, %d, want distinct non-zero values", got[0].id, got[1].id)
	}
}

func TestWriteBatchLarge(t *testing.T) {
	dsn := pgtest.Start(t)
	pool := newPool(t, dsn)
	w := pgstore.New(pool)

	const n = 10000
	tenant := uuid.New()
	base := time.Now().UTC()
	recs := make([]logrec.Record, n)
	for i := range recs {
		recs[i] = logrec.Record{
			TenantID: tenant,
			TS:       base.Add(time.Duration(i) * time.Microsecond),
			Level:    "info",
			Source:   "bench",
			Message:  fmt.Sprintf("row-%d", i),
			Attrs:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}

	if err := w.WriteBatch(context.Background(), recs); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM logs WHERE tenant_id = $1`, tenant).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Fatalf("stored %d rows, want %d", count, n)
	}
}

func TestWriteBatchRejectsMissingPartition(t *testing.T) {
	dsn := pgtest.Start(t)
	pool := newPool(t, dsn)
	w := pgstore.New(pool)

	// No partition exists for a timestamp years away: the write must report an
	// error rather than silently drop the batch.
	err := w.WriteBatch(context.Background(), []logrec.Record{{
		TenantID: uuid.New(),
		TS:       time.Now().UTC().AddDate(5, 0, 0),
		Message:  "far future",
	}})
	if err == nil {
		t.Fatal("WriteBatch succeeded for a timestamp with no partition, want an error")
	}
}
