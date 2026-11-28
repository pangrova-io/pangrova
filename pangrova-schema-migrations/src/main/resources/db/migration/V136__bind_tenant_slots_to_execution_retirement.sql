ALTER TABLE tenant_concurrency_queries
    ADD COLUMN execution_bound BOOLEAN NOT NULL DEFAULT FALSE;

-- The caller commits only a successful reservation. A conflicting query ID rolls back
-- the provisional charge with the rest of the admission transaction.
CREATE FUNCTION reserve_tenant_query_slot(
    requested_tenant VARCHAR, requested_query VARCHAR, capacity INTEGER, bind_execution BOOLEAN)
RETURNS BOOLEAN LANGUAGE SQL VOLATILE AS $$
    WITH reserved_tenant AS (
        INSERT INTO tenant_concurrency_slots (tenant_id, active_count)
        SELECT requested_tenant, 1 WHERE capacity > 0
        ON CONFLICT (tenant_id) DO UPDATE SET active_count = tenant_concurrency_slots.active_count + 1
        WHERE tenant_concurrency_slots.active_count < capacity
        RETURNING tenant_id
    ), reserved_query AS (
        INSERT INTO tenant_concurrency_queries (query_id, tenant_id, execution_bound)
        SELECT requested_query, tenant_id, bind_execution FROM reserved_tenant
        ON CONFLICT (query_id) DO NOTHING
        RETURNING query_id
    )
    SELECT EXISTS (SELECT 1 FROM reserved_query)
$$;

-- Lock the tenant first, then remove exactly the matching reservation. An inconsistent
-- count returns false so the caller can roll back the slot and the owning execution.
CREATE FUNCTION release_tenant_query_slot(
    requested_query VARCHAR, requested_tenant VARCHAR, bound_to_execution BOOLEAN)
RETURNS BOOLEAN LANGUAGE SQL VOLATILE AS $$
    WITH locked_tenant AS MATERIALIZED (
        SELECT s.tenant_id FROM tenant_concurrency_queries q
        JOIN tenant_concurrency_slots s USING (tenant_id)
        WHERE q.query_id = requested_query AND q.execution_bound = bound_to_execution
            AND (requested_tenant IS NULL OR q.tenant_id = requested_tenant)
        FOR UPDATE OF s
    ), removed AS (
        DELETE FROM tenant_concurrency_queries q USING locked_tenant t
        WHERE q.query_id = requested_query AND q.tenant_id = t.tenant_id
            AND q.execution_bound = bound_to_execution
        RETURNING q.tenant_id
    ), adjusted AS (
        UPDATE tenant_concurrency_slots s SET active_count = s.active_count - 1
        FROM removed WHERE s.tenant_id = removed.tenant_id AND s.active_count > 0
        RETURNING s.tenant_id
    )
    SELECT (SELECT COUNT(*) FROM removed) = (SELECT COUNT(*) FROM adjusted)
$$;
