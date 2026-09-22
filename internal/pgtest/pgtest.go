// Package pgtest starts a throwaway Postgres for integration tests.
//
// The schema is applied from db/migrations, the same files golang-migrate runs
// in production, so these tests cannot pass against a schema that has drifted
// from the migrations.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// partitionSpan is how many days either side of today get a partition, so
// tests writing records around "now" have somewhere to put them.
const partitionSpan = 1

// migrationsDir resolves db/migrations relative to this file, so it works
// whichever package directory the test binary runs in.
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("cannot resolve caller path")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "db", "migrations")
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err != nil {
		return "", fmt.Errorf("migrations directory %s: %w", abs, err)
	}
	return abs, nil
}

// applyMigrations runs every *.up.sql in lexical order, which is the order
// golang-migrate applies them.
func applyMigrations(ctx context.Context, conn *pgx.Conn) error {
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no *.up.sql files in %s", dir)
	}
	sort.Strings(files)

	for _, f := range files {
		sqlBytes, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Base(f), err)
		}
		if _, err := conn.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply %s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

// Start boots a Postgres container with db/migrations applied and partitions
// created around today, and returns its DSN. The container is terminated when
// the test finishes.
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

	setupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(setupCtx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(setupCtx)

	// CURRENT_DATE below is session-dependent; pin it to UTC so partitions
	// line up with the UTC timestamps the tests write.
	if _, err := conn.Exec(setupCtx, "SET TIME ZONE 'UTC'"); err != nil {
		t.Fatalf("set time zone: %v", err)
	}
	if err := applyMigrations(setupCtx, conn); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	// Uses the partition helper the migrations define, so the tests exercise
	// it rather than a hand-rolled copy.
	if _, err := conn.Exec(setupCtx,
		"SELECT logs_create_partitions_between(CURRENT_DATE - $1::int, CURRENT_DATE + $2::int)",
		partitionSpan, partitionSpan,
	); err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	return dsn
}
