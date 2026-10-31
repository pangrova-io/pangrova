-- CACHE > 1 permits an older pooled connection to return a token below one
-- already returned on another connection. Worker ownership requires increasing
-- tokens, including after release deletes the current execution_anchor row.
--
-- Drain anchor transactions before changing the allocator. ALTER takes the
-- sequence lock until commit; its last_value includes unused cached ranges.
-- PostgreSQL 16 rewrites storage for CACHE changes as well as RESTART. Explicitly
-- restart above the reserved and live high watermarks; the storage rewrite also
-- invalidates cached-but-unissued values in surviving pooled backends.
-- Preserve the sequence OID, owner and grants, and never reuse a reserved token.
LOCK TABLE execution_anchor IN ACCESS EXCLUSIVE MODE;
ALTER SEQUENCE execution_anchor_token_seq CACHE 1 NO CYCLE;

DO $$
DECLARE
    reserved_high_watermark BIGINT;
    live_high_watermark BIGINT;
BEGIN
    SELECT last_value INTO reserved_high_watermark FROM execution_anchor_token_seq;
    SELECT COALESCE(MAX(fencing_token), 0) INTO live_high_watermark FROM execution_anchor;
    reserved_high_watermark := GREATEST(reserved_high_watermark, live_high_watermark);
    IF reserved_high_watermark = 9223372036854775807 THEN
        RAISE EXCEPTION 'Execution fencing token space is exhausted';
    END IF;
    EXECUTE format('ALTER SEQUENCE execution_anchor_token_seq RESTART WITH %s CACHE 1 NO CYCLE',
            reserved_high_watermark + 1);
END;
$$;
