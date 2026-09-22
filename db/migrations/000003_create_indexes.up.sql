-- Indexes are created on the partitioned parent. PostgreSQL propagates each
-- of these as a "partitioned index" to every existing partition and to any
-- partition created later via logs_create_partition*.

-- Covering index for tenant + time-range selection with index-only scans
-- for the commonly-projected level/source columns.
CREATE INDEX logs_tenant_ts_covering_idx
    ON logs (tenant_id, ts DESC) INCLUDE (level, source);

-- Level filtering within a tenant, most recent first.
CREATE INDEX logs_tenant_level_ts_idx
    ON logs (tenant_id, level, ts DESC);

-- Containment/existence queries against attrs (e.g. attrs @> '{"k":"v"}').
CREATE INDEX logs_attrs_gin_idx
    ON logs USING GIN (attrs jsonb_path_ops);

-- Cheap, tiny index for large sequential range scans over ts.
CREATE INDEX logs_ts_brin_idx
    ON logs USING BRIN (ts);
