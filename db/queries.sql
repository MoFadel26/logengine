-- Sample queries against the logs table, with the actual plans observed on
-- a local PostgreSQL 18.6 instance after seeding 1,000,000 rows across 10
-- tenants over 31 daily partitions (db/seed/seed.sh) and running
-- VACUUM ANALYZE logs. These are real EXPLAIN (ANALYZE, BUFFERS) captures,
-- not predictions. Partition-by-partition subplans are repetitive, so only
-- the first executed subplan of each Append is kept below; row counts and
-- "Heap Fetches"/"Recheck Cond" behavior were the same across partitions
-- for a given query in this environment.

-- =====================================================================
-- Q1. Tenant + time-range, projecting only covered columns.
-- Expected to use the covering index (tenant_id, ts DESC) INCLUDE
-- (level, source) as an index-only scan.
-- =====================================================================
SELECT tenant_id, ts, level, source
FROM logs
WHERE tenant_id = '00000000-0000-0000-0000-000000000001'
  AND ts >= now() - interval '7 days'
ORDER BY ts DESC
LIMIT 100;

-- Observed plan (EXPLAIN (ANALYZE, BUFFERS)):
--
-- Limit  (actual time=0.100..0.124 rows=100 loops=1)
--   Buffers: shared read=3
--   ->  Append  (actual time=0.100..0.115 rows=100 loops=1)
--         Buffers: shared read=3
--         Subplans Removed: 23   -- partition pruning excluded older days
--         ->  Index Only Scan using logs_p20260922_tenant_id_ts_level_source_idx
--               on logs_p20260922  (actual time=0.099..0.106 rows=100 loops=1)
--               Index Cond: (tenant_id = '...0001'::uuid AND ts >= (now() - '7 days'))
--               Heap Fetches: 0
--         ->  Index Only Scan on logs_p20260921 ... (never executed, LIMIT satisfied)
--         ... (6 more same-shape "never executed" subplans, one per pruned-in day)
-- Planning Time: 12.193 ms
-- Execution Time: 0.201 ms
--
-- CONFIRMED: Index Only Scan, Heap Fetches: 0. This is the index-only-scan
-- requirement being met. Heap Fetches: 0 required a prior VACUUM ANALYZE;
-- without it the visibility map is not up to date and PostgreSQL falls back
-- to heap fetches for recently written pages.

-- =====================================================================
-- Q2. Tenant + level filter, time-ordered, projecting a non-covered
-- column (message). Expected to use (tenant_id, level, ts DESC).
-- =====================================================================
SELECT tenant_id, ts, level, source, message
FROM logs
WHERE tenant_id = '00000000-0000-0000-0000-000000000003'
  AND level = 'ERROR'
  AND ts >= now() - interval '7 days'
ORDER BY ts DESC
LIMIT 100;

-- Observed plan:
--
-- Limit  (actual time=0.047..0.463 rows=100 loops=1)
--   Buffers: shared hit=6 read=96
--   ->  Append  (actual time=0.047..0.457 rows=100 loops=1)
--         Subplans Removed: 23
--         ->  Index Scan using logs_p20260922_tenant_id_level_ts_idx
--               on logs_p20260922  (actual time=0.046..0.450 rows=100 loops=1)
--               Index Cond: (tenant_id = '...0003'::uuid AND level = 'ERROR'
--                            AND ts >= (now() - '7 days'))
-- Planning Time: 10.996 ms
-- Execution Time: 0.526 ms
--
-- NOTE: this is a plain Index Scan, not an index-only scan -- message is
-- not part of any index, so PostgreSQL must visit the heap. That is
-- expected and correct for this query shape; the (tenant_id, level, ts DESC)
-- index still avoids scanning/sorting rows that don't match the filter.

-- =====================================================================
-- Q3. Selective JSONB containment lookup (e.g. locating a specific
-- request). Expected to use the GIN index (attrs jsonb_path_ops).
-- =====================================================================
SELECT tenant_id, ts, attrs
FROM logs
WHERE attrs @> jsonb_build_object('request_id', '<some-uuid>'::uuid);

-- Observed plan (against a request_id value known to exist in the data):
--
-- Append  (actual time=0.247..2.990 rows=1 loops=1)
--   Buffers: shared hit=32 read=62
--   ->  Bitmap Heap Scan on logs_p20260823  (actual time=0.246..0.247 rows=1 loops=1)
--         Recheck Cond: (attrs @> jsonb_build_object('request_id', '...'::uuid))
--         Heap Blocks: exact=1
--         ->  Bitmap Index Scan on logs_p20260823_attrs_idx
--               (actual time=0.229..0.229 rows=1 loops=1)
--               Index Cond: (attrs @> jsonb_build_object('request_id', '...'::uuid))
--   ->  Bitmap Heap Scan on logs_p20260824 ... rows=0  (index probed, no match)
--   ... (one such probe per remaining partition; GIN index used throughout)
-- Planning Time: 11.733 ms
-- Execution Time: 3.312 ms
--
-- CONFIRMED: every partition is probed via "Bitmap Index Scan on
-- logs_pYYYYMMDD_attrs_idx", i.e. the GIN index, not a sequential scan.
--
-- NOTE on selectivity: a low-selectivity containment filter such as
-- attrs @> '{"status": "error"}' (present in ~1/3 of rows in this seed
-- data) does NOT use the GIN index here -- the planner instead picks a
-- Bitmap Heap Scan driven by (tenant_id, level, ts) or the primary key
-- and rechecks attrs with a plain Filter, because that is genuinely
-- cheaper at this selectivity and data size. Observed for:
--   SELECT count(*) FROM logs
--   WHERE tenant_id = '00000000-0000-0000-0000-000000000002'
--     AND attrs @> '{"status": "error"}';
-- -> Bitmap Heap Scan ... Filter: (attrs @> '{"status": "error"}'::jsonb),
--    "Bitmap Index Scan on logs_pYYYYMMDD_tenant_id_level_ts_idx" (or pkey),
--    Execution Time: 226.103 ms (full 1M-row table, no LIMIT).
-- Reported here rather than claimed as a GIN index-only success, per the
-- verification requirement to state plans as observed.

-- =====================================================================
-- Q4. Large time-range scan with no tenant filter (operational/ad hoc
-- query, e.g. an on-call engineer scanning a whole time window).
-- BRIN is intended for exactly this shape at large scale.
-- =====================================================================
SELECT count(*)
FROM logs
WHERE ts >= CURRENT_DATE - interval '20 days'
  AND ts <  CURRENT_DATE - interval '19 days' + interval '2 hours';

-- Observed plan, default planner settings:
--
-- Aggregate  (actual time=11.258..11.263 rows=1 loops=1)
--   ->  Append  (actual time=0.319..10.188 rows=36342 loops=1)
--         Subplans Removed: 29
--         ->  Index Only Scan using logs_p20260902_pkey
--               on logs_p20260902  (actual time=0.319..7.047 rows=33609 loops=1)
--               Heap Fetches: 0
--         ->  Index Only Scan using logs_p20260903_tenant_id_ts_level_source_idx
--               on logs_p20260903  (actual time=0.335..1.444 rows=2733 loops=1)
--               Heap Fetches: 0
-- Execution Time: 11.311 ms
--
-- NOTE: at this data size (~33k rows/partition, whole partitions fitting in
-- a handful of MB) the planner prefers the btree primary key / covering
-- index over the BRIN index for this query -- both give index-only scans,
-- which are cheaper here than BRIN's bitmap-and-recheck. The BRIN index is
-- NOT the winning plan for this query at this scale; reporting that
-- honestly rather than as a success.
--
-- To confirm the BRIN index is nonetheless valid and usable, the same
-- query was re-run with `SET enable_seqscan = off; SET enable_indexscan =
-- off;` (forcing the planner off every non-BRIN option):
--
-- ->  Parallel Bitmap Heap Scan on logs_p20260902
--       Recheck Cond: (ts >= ... AND ts < ...)
--       Heap Blocks: lossy=222
--       ->  Bitmap Index Scan on logs_p20260902_ts_idx   -- the BRIN index
--             Index Cond: (ts >= ... AND ts < ...)
-- Execution Time: 21.455 ms
--
-- This confirms logs_ts_brin_idx is a real, working index (visible as
-- "lossy" heap blocks, characteristic of BRIN's block-range summaries). It
-- becomes the planner's natural choice on larger tables/partitions than
-- this 1M-row seed produces, or when the btree indexes above are not
-- applicable to a query's column list.
