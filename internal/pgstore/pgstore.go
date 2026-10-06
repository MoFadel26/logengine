// Package pgstore writes batches of log records to Postgres with COPY
// and queries records against the partitioned logs table.
package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MoFadel26/logengine/internal/logrec"
)

// columns matches the logs table. id is GENERATED ALWAYS AS IDENTITY and must
// not be supplied.
var columns = []string{"tenant_id", "ts", "level", "source", "message", "attrs"}

// Writer persists and queries log records against the partitioned logs table.
type Writer struct {
	pool *pgxpool.Pool
}

// Store is an alias for Writer.
type Store = Writer

// New wraps a pgx pool.
func New(pool *pgxpool.Pool) *Writer { return &Writer{pool: pool} }

// QueryParams configures filters for searching logs.
type QueryParams struct {
	TenantID *uuid.UUID
	Level    string
	Source   string
	Search   string
	From     time.Time
	To       time.Time
	Limit    int
}

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

// Query searches logs matching the given parameters.
func (w *Writer) Query(ctx context.Context, p QueryParams) ([]logrec.Record, error) {
	var (
		clauses []string
		args    []any
	)

	if p.TenantID != nil {
		args = append(args, *p.TenantID)
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if p.Level != "" {
		args = append(args, strings.ToUpper(p.Level))
		clauses = append(clauses, fmt.Sprintf("level = $%d", len(args)))
	}
	if p.Source != "" {
		args = append(args, p.Source)
		clauses = append(clauses, fmt.Sprintf("source = $%d", len(args)))
	}
	if p.Search != "" {
		args = append(args, "%"+p.Search+"%")
		clauses = append(clauses, fmt.Sprintf("message ILIKE $%d", len(args)))
	}
	if !p.From.IsZero() {
		args = append(args, p.From)
		clauses = append(clauses, fmt.Sprintf("ts >= $%d", len(args)))
	}
	if !p.To.IsZero() {
		args = append(args, p.To)
		clauses = append(clauses, fmt.Sprintf("ts <= $%d", len(args)))
	}

	query := "SELECT tenant_id, ts, coalesce(level, ''), coalesce(source, ''), coalesce(message, ''), attrs FROM logs"
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY ts DESC"

	limit := p.Limit
	if limit <= 0 {
		limit = 100
	} else if limit > 1000 {
		limit = 1000
	}
	args = append(args, limit)
	query += fmt.Sprintf(" LIMIT $%d", len(args))

	rows, err := w.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	var records []logrec.Record
	for rows.Next() {
		var (
			rec   logrec.Record
			attrs *[]byte
		)
		if err := rows.Scan(&rec.TenantID, &rec.TS, &rec.Level, &rec.Source, &rec.Message, &attrs); err != nil {
			return nil, fmt.Errorf("scan log row: %w", err)
		}
		if attrs != nil {
			rec.Attrs = *attrs
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate log rows: %w", err)
	}
	return records, nil
}
