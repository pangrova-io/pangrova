-- A committed definition change invalidates exactly the affected running stream keys.
CREATE FUNCTION notify_running_stream_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.lifecycle_state = 'RUNNING' THEN
            PERFORM pg_notify('pangrova_running_stream_' || TG_RELID::text, OLD.stream_name);
        END IF;
        RETURN NULL;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF OLD.lifecycle_state = 'RUNNING'
                AND (NEW.lifecycle_state <> 'RUNNING' OR OLD.stream_name <> NEW.stream_name) THEN
            PERFORM pg_notify('pangrova_running_stream_' || TG_RELID::text, OLD.stream_name);
        END IF;
    END IF;

    IF NEW.lifecycle_state = 'RUNNING' THEN
        PERFORM pg_notify('pangrova_running_stream_' || TG_RELID::text, NEW.stream_name);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER notify_running_stream_change
AFTER INSERT OR DELETE OR UPDATE OF stream_name, lifecycle_state, tenant_id, checkpoint_id,
    epoch, target_table, source_provider, source_properties, source_format, stream_properties,
    creator_identity, schema_version ON ingestion_stream_state
FOR EACH ROW EXECUTE FUNCTION notify_running_stream_change();
