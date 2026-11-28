-- Keep one client round trip and take the scope snapshot after the anchor lock.
CREATE FUNCTION shuffle.seal_terminal_cleanup(
    execution_key text,
    expected_owner text,
    expected_incarnation text,
    expected_token bigint
) RETURNS integer
LANGUAGE plpgsql VOLATILE SET search_path TO pg_catalog AS $$
DECLARE
    lease_expires_at timestamptz;
    drain_state integer;
    matching_scope boolean;
BEGIN
    SELECT anchor.expires_at, anchor.recovery_drain_state
      INTO lease_expires_at, drain_state
      FROM public.execution_anchor anchor
      WHERE anchor.execution_id = execution_key
        AND anchor.owner_id = expected_owner
        AND anchor.owner_incarnation = expected_incarnation
        AND anchor.fencing_token = expected_token
      FOR UPDATE;
    IF NOT FOUND OR drain_state <> 0 OR lease_expires_at <= clock_timestamp() THEN
        RETURN -1;
    END IF;

    -- Registration holds the same anchor in share mode. Its newly inserted scope
    -- is visible to this next statement even when the outer call started earlier.
    SELECT scope.owner_id = expected_owner
           AND scope.owner_incarnation = expected_incarnation
           AND scope.owner_fencing_token = expected_token
      INTO matching_scope
      FROM shuffle.shuffle_query_scope scope
      WHERE scope.query_id = execution_key
      FOR UPDATE;
    IF FOUND AND matching_scope IS DISTINCT FROM TRUE THEN
        RETURN 0;
    END IF;
    IF lease_expires_at <= clock_timestamp() THEN
        RETURN -1;
    END IF;

    INSERT INTO shuffle.shuffle_cleanup_intent (query_id, revocation_generation)
    VALUES (execution_key, expected_token)
    ON CONFLICT (query_id) DO NOTHING;
    RETURN 1;
END;
$$;
