# T3 Code Master Prompt — Go Real-Time Log Analytics Engine

## Role
You are the orchestrator. Run **three agents in parallel**, each in its **own git worktree and branch**. Do not run tracks sequentially. Do not let agents edit files outside their track.

## Model Assignment
| Thread | Model |
|---|---|
| Orchestrator (setup, PR review, merge) | Claude Opus 5 |
| Agent 1 — PostgreSQL Schema | Claude Sonnet 5 |
| Agent 2 — Go API + Worker Pool | Claude Opus 5 |
| Agent 3 — Docker + Kubernetes | Claude Sonnet 5 |

Start each agent thread with its assigned model.

## Environment
- Host: MacBook M1 Pro (`darwin/arm64`)
- Go 1.23+, PostgreSQL 16, Docker (buildx), Kubernetes (kind or k3d locally)
- Images must build for `linux/arm64` and `linux/amd64`

## Repo Layout (contract shared by all agents)
```
/db/            -> Agent 1
/cmd/api/       -> Agent 2
/internal/      -> Agent 2
/deploy/docker/ -> Agent 3
/deploy/k8s/    -> Agent 3
```

## Shared Contracts (fixed before agents start)
- Log payload:
  ```json
  { "tenant_id": "uuid", "ts": "RFC3339Nano", "level": "string", "source": "string", "message": "string", "attrs": {} }
  ```
- API port: `8080`. Health: `GET /healthz` (liveness), `GET /readyz` (readiness).
- Env vars: `DATABASE_URL`, `WORKER_COUNT`, `QUEUE_SIZE`, `SHUTDOWN_TIMEOUT` (default `25s`).
- App must exit cleanly on `SIGTERM` within `SHUTDOWN_TIMEOUT`.

## Worktree Setup (execute first, then launch all agents at once)
```bash
git checkout main && git pull
git worktree add ../logengine-schema  -b feat/pg-schema
git worktree add ../logengine-api     -b feat/go-api
git worktree add ../logengine-deploy  -b feat/docker-k8s
```

---

## Agent 1 — PostgreSQL Schema
**Worktree:** `../logengine-schema` · **Branch:** `feat/pg-schema` · **Scope:** `/db/` only

Tasks:
1. Create `logs` table with composite primary key `(tenant_id, ts, id)`.
   - `tenant_id UUID NOT NULL`, `ts TIMESTAMPTZ NOT NULL`, `id BIGINT GENERATED ALWAYS AS IDENTITY`
   - `level TEXT`, `source TEXT`, `message TEXT`, `attrs JSONB`
2. Range-partition by `ts` (daily). Add a function to create future partitions.
3. Indexes for **selection** and **projection**:
   - Covering index: `(tenant_id, ts DESC) INCLUDE (level, source)` for index-only scans.
   - `(tenant_id, level, ts DESC)` for level filters.
   - GIN on `attrs` (`jsonb_path_ops`).
   - BRIN on `ts` for large range scans.
4. Migrations in `/db/migrations` (golang-migrate format, up + down).
5. `/db/queries.sql` with sample tenant+time-range queries and their `EXPLAIN ANALYZE` expectations.
6. Seed script for 1M rows across 10 tenants.

Done when: migrations apply and roll back cleanly; sample queries use index-only scans.

---

## Agent 2 — Go REST API + Worker Pool
**Worktree:** `../logengine-api` · **Branch:** `feat/go-api` · **Scope:** `/cmd/api/`, `/internal/`, `go.mod`

Tasks:
1. `POST /v1/logs` accepts a single object or array. Validate against the shared contract.
2. Push validated logs into a **buffered channel** (`QUEUE_SIZE`).
   - Channel full → return `503` with `Retry-After`. Never block the handler.
3. **Worker pool** (`WORKER_COUNT` goroutines) consumes the channel, batches (size or time flush), writes with `pgx` `CopyFrom`.
4. **Graceful shutdown** on `SIGTERM`/`SIGINT` via `signal.NotifyContext`:
   1. Set readiness to failing (`/readyz` → 503).
   2. `http.Server.Shutdown(ctx)` — stop accepting requests.
   3. `close(channel)` — workers drain remaining items.
   4. `sync.WaitGroup` wait, bounded by `SHUTDOWN_TIMEOUT`.
   5. Flush final batches, close DB pool, exit 0.
5. Structured logging (`log/slog`), Prometheus metrics (queue depth, batch latency, dropped count).
6. Tests: unit tests for handler/backpressure, integration test with testcontainers Postgres, shutdown test proving zero loss of accepted logs. Run with `-race`.

Done when: `go test -race ./...` passes; SIGTERM under load loses no accepted logs.

---

## Agent 3 — Docker + Kubernetes
**Worktree:** `../logengine-deploy` · **Branch:** `feat/docker-k8s` · **Scope:** `/deploy/` only

### Dockerfile (`/deploy/docker/Dockerfile`)
1. Stage 1: `golang:1.23-alpine`, `CGO_ENABLED=0`, use `--platform=$BUILDPLATFORM` with `TARGETOS/TARGETARCH` for cross-builds from M1.
2. Stage 2: `gcr.io/distroless/static:nonroot`.
3. **Signal propagation:** use exec-form `ENTRYPOINT ["/app/api"]` so the binary is PID 1 and receives `SIGTERM` directly. No shell wrapper. If a wrapper is ever needed, use `tini`.
4. `STOPSIGNAL SIGTERM`, non-root user, `EXPOSE 8080`.
5. `docker buildx build --platform linux/arm64,linux/amd64` command in a `Makefile`.

### Kubernetes (`/deploy/k8s/`)
1. `Deployment` with **zero-downtime rolling update**:
   - `strategy.rollingUpdate: maxUnavailable: 0, maxSurge: 1`
   - `minReadySeconds: 5`
   - `readinessProbe` on `/readyz`, `livenessProbe` on `/healthz`, `startupProbe`
   - `lifecycle.preStop: sleep 5` (lets endpoints deregister before SIGTERM)
   - `terminationGracePeriodSeconds: 30` (greater than `SHUTDOWN_TIMEOUT`)
2. **Shared network namespace:** add a sidecar (e.g. log/metrics forwarder) in the same pod reaching the API over `localhost:8080`. Document that pod containers share one network namespace.
3. `Service` (ClusterIP), `PodDisruptionBudget` (`minAvailable: 1`), `HorizontalPodAutoscaler`, `ConfigMap` + `Secret` for env vars.
4. Kustomize base + `overlays/local` for kind/k3d.

Done when: `kubectl rollout restart` under load shows zero failed requests (verify with `hey` or `k6`).

---

## Merge Plan (T3 Code PR flow)
1. Each agent commits on its branch and opens a PR to `main` through T3 Code's built-in PR feature.
2. Merge order: `feat/pg-schema` → `feat/go-api` → `feat/docker-k8s`.
3. Before each merge: rebase on `main`, run `go test -race ./...`, `docker buildx build`, `kubectl apply --dry-run=server`.
4. After all merges: run end-to-end test (kind cluster + Postgres + load test + rolling restart).
5. Remove worktrees:
   ```bash
   git worktree remove ../logengine-schema
   git worktree remove ../logengine-api
   git worktree remove ../logengine-deploy
   ```

## Rules for All Agents
- Stay inside your scope. Contract changes require orchestrator approval.
- Small commits, conventional commit messages.
- Report blockers immediately; do not guess across tracks.
