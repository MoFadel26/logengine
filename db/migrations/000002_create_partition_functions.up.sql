-- Idempotent helpers for managing daily range partitions of logs.

-- Creates the single daily partition covering [p_day, p_day + 1 day) if it
-- does not already exist. Safe to call repeatedly.
CREATE OR REPLACE FUNCTION logs_create_partition(p_day date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name text := 'logs_p' || to_char(p_day, 'YYYYMMDD');
    start_ts timestamptz := p_day::timestamptz;
    end_ts timestamptz := (p_day + 1)::timestamptz;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
          AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_ts, end_ts
        );
    END IF;
END;
$$;

-- Creates daily partitions for every day in [p_start, p_end] (inclusive).
-- Idempotent: relies on logs_create_partition's own existence check.
CREATE OR REPLACE FUNCTION logs_create_partitions_between(p_start date, p_end date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    d date;
BEGIN
    IF p_start > p_end THEN
        RAISE EXCEPTION 'p_start (%) must be <= p_end (%)', p_start, p_end;
    END IF;

    d := p_start;
    WHILE d <= p_end LOOP
        PERFORM logs_create_partition(d);
        d := d + 1;
    END LOOP;
END;
$$;

-- Convenience wrapper for creating upcoming partitions ahead of time, e.g.
-- from a daily cron job: creates today's partition plus p_days_ahead more.
-- Idempotent, like the functions it wraps.
CREATE OR REPLACE FUNCTION logs_create_future_partitions(p_days_ahead int DEFAULT 7)
RETURNS void
LANGUAGE sql
AS $$
    SELECT logs_create_partitions_between(CURRENT_DATE, CURRENT_DATE + p_days_ahead);
$$;
