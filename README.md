# logengine

A real-time log analytics engine in Go: a bounded-queue ingest API in front of a
daily-partitioned PostgreSQL store, packaged as a distroless multi-arch image
with Kubernetes manifests tuned for zero-downtime rollouts.

## How it works

```
POST /v1/logs ──▶ validate ──▶ buffered channel ──▶ worker pool ──▶ CopyFrom ──▶ logs
                                 (QUEUE_SIZE)       (WORKER_COUNT)              (partitioned)
                                      │
                                   full? ──▶ 503 + Retry-After
```

The handler never blocks. Records go into a buffered channel with a
non-blocking send; if the channel is full the request is refused with `503` and
a `Retry-After` header rather than backing traffic up. Workers batch by size
(500 records) or time (500ms), whichever comes first, and write with pgx
`CopyFrom`.

A failed write is retried with exponential backoff (100ms doubling to a 5s
ceiling) instead of being dropped, so a transient database outage costs latency
and turns into backpressure rather than data loss.

**Shutdown** on `SIGTERM`/`SIGINT` runs in a fixed order: readiness starts
failing, the HTTP server drains in-flight requests, the queue closes, workers
flush what remains within `SHUTDOWN_TIMEOUT`, then the pool closes. Anything
still unwritten when that budget expires is counted, logged, and the process
exits non-zero — losing records is never a silent, zero-exit outcome.

## Layout

| Path | Contents |
|---|---|
| `cmd/api/` | Service entrypoint |
| `internal/` | Handler, queue, worker pool, pgx store, config, metrics, embedded UI |
| `db/migrations/` | golang-migrate up/down pairs |
| `db/seed/` | Synthetic data generator |
| `db/queries.sql` | Sample queries with observed `EXPLAIN ANALYZE` plans |
| `deploy/docker/` | Multi-arch distroless Dockerfile |
| `deploy/k8s/` | Kustomize base + local overlay ([details](deploy/k8s/README.md)) |

## Quick start

### Kubernetes (kind/k3d)

The local overlay includes a Postgres StatefulSet, so it comes up unaided:

```bash
kind create cluster --name logengine
docker build -t logengine-api:dev -f deploy/docker/Dockerfile .
kind load docker-image logengine-api:dev --name logengine
kubectl apply -k deploy/k8s/overlays/local
kubectl -n logengine-local rollout status statefulset/postgres
```

Then apply the schema and create partitions:

```bash
kubectl -n logengine-local port-forward svc/postgres 55432:5432 &
export DATABASE_URL='postgres://logengine:logengine@127.0.0.1:55432/logengine?sslmode=disable'\nmigrate -path db/migrations -database "$DATABASE_URL" up
psql "$DATABASE_URL" -c 'SELECT logs_create_partitions_between(CURRENT_DATE - 2, CURRENT_DATE + 2);'
```

Writes fail if no partition covers the record's timestamp, so create partitions
ahead of ingest. `logs_create_future_partitions(days_ahead)` is meant for a
daily cron.

### Locally against your own Postgres

```bash
export DATABASE_URL='postgres://user@localhost:5432/logengine?sslmode=disable'
migrate -path db/migrations -database "$DATABASE_URL" up
psql "$DATABASE_URL" -c 'SELECT logs_create_future_partitions(7);'
go run ./cmd/api
```

Seed a million rows across ten tenants (`ROWS`, `TENANTS`, `DAYS_BACK` override
the defaults):

```bash
DATABASE_URL="$DATABASE_URL" ./db/seed/seed.sh
```

## API & Web UI

The service listens on `:8080`. In Kubernetes the Service fronts it on port
`80`, so in-cluster callers use `http://logengine-api/v1/logs`.

| Endpoint | Purpose |
|---|---|
| `GET /` or `GET /ui/` | Embedded web analytics UI dashboard |
| `GET /v1/logs` | Query and filter logs |
| `POST /v1/logs` | Ingest one record or an array of them |
| `GET /healthz` | Liveness |
| `GET /readyz` | Readiness — `503` once shutdown begins |
| `GET /metrics` | Prometheus exposition |

### Web UI Dashboard

Navigate to `http://localhost:8080/` (or `/ui/`) in any browser:
- **Search & Filter:** Filter by `tenant_id`, severity `level` (`INFO`, `WARN`, `ERROR`), `source`, time ranges (`15m`, `1h`, `24h`, `7d`, `30d`), or message text search.
- **Auto-Refresh / Live Tail:** Toggle 3s, 5s, or 10s polling for continuous inspection.
- **Inspect Attributes:** Expand any log line to view JSON attributes and metadata.
- **Interactive Tester:** Send test logs directly from the modal to verify ingestion and queue batching.

### Query API

`GET /v1/logs` accepts the following query parameters:
- `tenant_id`: UUID (e.g. `00000000-0000-0000-0000-000000000001`)
- `level`: Log level (`INFO`, `WARN`, `ERROR`, `DEBUG`)
- `source`: Originating service name
- `q`: Case-insensitive substring match on message
- `from` / `to`: Relative duration (`15m`, `1h`, `24h`, `7d`) or RFC3339 timestamp
- `limit`: Number of rows to return (default 100, max 1000)

```bash
curl "http://localhost:8080/v1/logs?level=ERROR&from=24h&limit=50"
```

### Ingest API

A record is `tenant_id` (UUID) and `ts` (RFC3339Nano), both required, plus
optional `level`, `source`, `message` and an `attrs` object:

```bash
curl -X POST localhost:8080/v1/logs -H 'Content-Type: application/json' -d '{
  "tenant_id": "00000000-0000-0000-0000-000000000001",
  "ts": "2026-09-22T12:00:00.123456789Z",
  "level": "INFO",
  "source": "svc-api",
  "message": "request handled",
  "attrs": {"request_id": "abc", "duration_ms": 12}
}'
```

| Status | Meaning |
|---|---|
| `202` | `{"accepted": n}` — queued |
| `400` | Payload failed validation |
| `413` | Body over 4 MiB |
| `503` | Queue full; `Retry-After` set, body reports `accepted` and `dropped` |

Accepted records in a partially refused array are always a prefix of what was
submitted, so a client knows exactly what to resend.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | *(required)* | Postgres connection string |
| `WORKER_COUNT` | `4` | Worker goroutines draining the queue |
| `QUEUE_SIZE` | `10000` | Buffered channel capacity |
| `SHUTDOWN_TIMEOUT` | `25s` | Budget for draining and final flush |

`terminationGracePeriodSeconds` in the manifests must exceed the preStop sleep
plus `SHUTDOWN_TIMEOUT`; it is set to 40s against a 5s + 25s bound.

## Schema

`logs` is range-partitioned by `ts` into daily partitions, keyed on
`(tenant_id, ts, id)` with `id BIGINT GENERATED ALWAYS AS IDENTITY`. Indexes
live on the parent and propagate to every partition:

- `(tenant_id, ts DESC) INCLUDE (level, source)` — covering, for index-only scans
- `(tenant_id, level, ts DESC)` — level filters
- GIN on `attrs` with `jsonb_path_ops` — selective JSONB lookups
- BRIN on `ts` — large range scans

`db/queries.sql` records the plans actually observed for four representative
queries, including where the planner does *not* pick GIN or BRIN and why that
is the right call at that selectivity and table size.

## Observability

Structured JSON logs via `log/slog`. Prometheus metrics on `/metrics`:

| Metric | Meaning |
|---|---|\n| `logengine_queue_depth` | Current queue occupancy |
| `logengine_logs_accepted_total` | Records queued |
| `logengine_logs_dropped_total` | Records refused — queue full |
| `logengine_logs_lost_total` | Accepted records abandoned unwritten |
| `logengine_rows_written_total` | Records persisted |
| `logengine_batch_write_errors_total` | Failed batch writes |
| `logengine_batch_write_duration_seconds` | Write latency histogram |

`logs_accepted_total` equalling `rows_written_total` with `logs_lost_total` at
zero is the zero-loss invariant.

## Development

```bash
go test -race ./...          # integration tests need a Docker daemon
go test -short ./...         # unit tests only
```

Integration tests start a throwaway Postgres with testcontainers and apply
`db/migrations` directly, so they cannot pass against a schema that has drifted
from the migrations.

## Building

```bash
make buildx                  # linux/arm64 + linux/amd64
```

A multi-platform build produces a manifest list, which cannot be loaded into
the local daemon — `make buildx` validates both targets and leaves the result
in the build cache. Add `--push` with a registry tag to publish it, or build a
single platform with `--load` for a runnable local image.

The build stage cross-compiles with `TARGETOS`/`TARGETARCH` from
`$BUILDPLATFORM`, so an arm64 host builds both targets natively rather than
emulating. The runtime stage is `gcr.io/distroless/static:nonroot` with an
exec-form `ENTRYPOINT`, so the binary is PID 1 and receives `SIGTERM` directly.

## Requirements

Go 1.27+, PostgreSQL 16+, Docker with buildx, and kubectl with kind or k3d for
the local cluster. `golang-migrate` is needed to apply migrations.
