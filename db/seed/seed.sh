#!/usr/bin/env bash
# Seeds the logs table with synthetic data for local verification.
#
# Generates rows server-side with generate_series and streams them into
# logs via COPY (no row-by-row client inserts). Two psql processes are
# piped together: the first runs `COPY (SELECT ...) TO STDOUT` to produce
# rows, the second runs `COPY logs FROM STDIN` to load them.
#
# Env vars:
#   DATABASE_URL  postgres connection string (required)
#   ROWS          total rows to generate (default 1000000)
#   TENANTS       number of tenants to spread rows across (default 10)
#   DAYS_BACK     spread timestamps over the last N days (default 30)
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL, e.g. postgres://fadel@localhost:5432/logengine_dev?sslmode=disable}"
ROWS="${ROWS:-1000000}"
TENANTS="${TENANTS:-10}"
DAYS_BACK="${DAYS_BACK:-30}"

echo "Ensuring partitions exist for the last ${DAYS_BACK} days..."
psql "$DATABASE_URL" -X -q -c \
  "SELECT logs_create_partitions_between(CURRENT_DATE - ${DAYS_BACK}, CURRENT_DATE);"

echo "Seeding ${ROWS} rows across ${TENANTS} tenants..."
time (
  psql "$DATABASE_URL" -X -q <<SQL | psql "$DATABASE_URL" -X -q -c "\copy logs (tenant_id, ts, level, source, message, attrs) FROM STDIN"
\copy (SELECT (ARRAY['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000004','00000000-0000-0000-0000-000000000005','00000000-0000-0000-0000-000000000006','00000000-0000-0000-0000-000000000007','00000000-0000-0000-0000-000000000008','00000000-0000-0000-0000-000000000009','00000000-0000-0000-0000-000000000010']::uuid[])[1 + (i % ${TENANTS})] AS tenant_id, now() - (random() * interval '1 day' * ${DAYS_BACK}) AS ts, (ARRAY['DEBUG','INFO','WARN','ERROR'])[1 + floor(random() * 4)::int] AS level, (ARRAY['svc-api','svc-worker','svc-gateway','svc-auth','svc-billing'])[1 + floor(random() * 5)::int] AS source, 'sample log message ' || i AS message, jsonb_build_object('request_id', gen_random_uuid(), 'duration_ms', floor(random() * 500)::int, 'status', (ARRAY['ok','timeout','error'])[1 + floor(random() * 3)::int]) AS attrs FROM generate_series(1, ${ROWS}) AS s(i)) TO STDOUT
SQL
)

echo "Done. Row count:"
psql "$DATABASE_URL" -X -q -c "SELECT count(*) FROM logs;"
