-- Partitioned logs table. Partitioned by range on ts (daily partitions are
-- created separately via the logs_create_partition* functions, see
-- 000002_create_partition_functions). The partition key (ts) is included in
-- the primary key, as required by PostgreSQL for partitioned tables.
CREATE TABLE logs (
    tenant_id UUID NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    id        BIGINT GENERATED ALWAYS AS IDENTITY,
    level     TEXT,
    source    TEXT,
    message   TEXT,
    attrs     JSONB,
    PRIMARY KEY (tenant_id, ts, id)
) PARTITION BY RANGE (ts);
