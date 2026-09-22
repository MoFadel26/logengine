// Package pgstore writes batches of log records to Postgres with COPY.
package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MoFadel26/logengine/internal/logrec"
)

// columns matches the logs table. id is GENERATED ALWAYS AS IDENTITY and must
// not be supplied.
var columns = []string{"tenant_id", "ts", "level", "source", "message", "attrs"}

// Writer persists log records into the partitioned logs table.
type Writer struct {
	pool *pgxpool.Pool
}

// New wraps a pgx pool.
func New(pool *pgxpool.Pool) *Writer { return &Writer{pool: pool} }

// WriteBatch COPYs recs into logs.
func (w *Writer) WriteBatch(ctx context.Context, recs []logrec.Record) error {
	src := pgx.CopyFromSlice(len(recs), func(i int) ([]any, error) {
		r := recs[i]
		var attrs any
		if r.Attrs != nil {
			attrs = r.Attrs
		}
		return []any{r.TenantID, r.TS, r.Level, r.Source, r.Message, attrs}, nil
	})

	n, err := w.pool.CopyFrom(ctx, pgx.Identifier{"logs"}, columns, src)
	if err != nil {
		return fmt.Errorf("copy %d rows into logs: %w", len(recs), err)
	}
	if int(n) != len(recs) {
		return fmt.Errorf("copy into logs wrote %d of %d rows", n, len(recs))
	}
	return nil
}
