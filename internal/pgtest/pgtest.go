// Package pgtest starts a throwaway Postgres for integration tests.
//
// The DDL mirrors the target logs schema (owned by the schema track) so these
// tests exercise the same shape the service writes to in production.
package pgtest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const schemaDDL = `
SET TIME ZONE 'UTC';

CREATE TABLE logs (
    tenant_id UUID        NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    id        BIGINT      GENERATED ALWAYS AS IDENTITY,
    level     TEXT,
    source    TEXT,
    message   TEXT,
    attrs     JSONB,
    PRIMARY KEY (tenant_id, ts, id)
) PARTITION BY RANGE (ts);

DO $$
DECLARE
    d date;
BEGIN
    FOR d IN
        SELECT generate_series(
            (now() AT TIME ZONE 'UTC')::date - 1,
            (now() AT TIME ZONE 'UTC')::date + 1,
            interval '1 day'
        )::date
    LOOP
        EXECUTE format(
            'CREATE TABLE logs_%s PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYYMMDD'),
            to_char(d, 'YYYY-MM-DD') || ' 00:00:00+00',
            to_char(d + 1, 'YYYY-MM-DD') || ' 00:00:00+00'
        );
    END LOOP;
END $$;
`

// Start boots a Postgres container with the logs table created and returns its
// DSN. The container is terminated when the test finishes.
func Start(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("logengine"),
		postgres.WithUsername("logengine"),
		postgres.WithPassword("logengine"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(setupCtx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(setupCtx)
	if _, err := conn.Exec(setupCtx, schemaDDL); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return dsn
}
