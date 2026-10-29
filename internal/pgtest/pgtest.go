// Package pgtest provides a throwaway Postgres database for integration tests.
//
// The schema is applied from db/migrations, the same files golang-migrate runs
// in production, so these tests cannot pass against a schema that has drifted
// from the migrations.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

// Start returns the DSN of a fresh database with db/migrations applied and
// partitions created around today. If PGTEST_DATABASE_URL is set, the database
// is created on that server and dropped when the test finishes; otherwise a
// Postgres container is started and terminated when the test finishes.
func Start(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	var dsn string
	if baseURL := os.Getenv("PGTEST_DATABASE_URL"); baseURL != "" {
		dsn = createDatabase(ctx, t, baseURL)
	} else {
		dsn = startContainer(ctx, t)
	}

	setupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(setupCtx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(setupCtx) }()

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

// startContainer boots a Postgres container, terminates it when the test
// finishes, and returns its DSN.
func startContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
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
	return dsn
}

// createDatabase creates a uniquely named database on the server at baseURL,
// drops it when the test finishes, and returns its DSN. The random name keeps
// parallel tests and packages from colliding on a shared server.
func createDatabase(ctx context.Context, t *testing.T, baseURL string) string {
	t.Helper()
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse PGTEST_DATABASE_URL: %v", err)
	}
	name := "pgtest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ident := pgx.Identifier{name}.Sanitize()

	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to PGTEST_DATABASE_URL: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(ctx, baseURL)
		if err != nil {
			t.Logf("connect to drop database %s: %v", name, err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)"); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})

	u.Path = "/" + name
	return u.String()
}
