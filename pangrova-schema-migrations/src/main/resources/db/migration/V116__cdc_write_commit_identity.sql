-- A CDC ingress commit is replayable only while it retains the exact source
-- ownership and offset interval that produced its rows. The identity is
-- optional for ordinary writes and all-or-nothing for CDC writes.
ALTER TABLE write_commit_journal
    ADD COLUMN cdc_stream_id VARCHAR(255),
    ADD COLUMN cdc_source_epoch VARCHAR(255),
    ADD COLUMN cdc_offset_ranges JSONB,
    ADD CONSTRAINT chk_write_cdc_binding CHECK (
        (cdc_stream_id IS NULL AND cdc_source_epoch IS NULL AND cdc_offset_ranges IS NULL)
        OR (cdc_stream_id IS NOT NULL AND btrim(cdc_stream_id) <> ''
            AND cdc_source_epoch IS NOT NULL AND btrim(cdc_source_epoch) <> ''
            AND cdc_offset_ranges IS NOT NULL
            AND jsonb_typeof(cdc_offset_ranges) = 'array'
            AND jsonb_array_length(cdc_offset_ranges) > 0)
    );

CREATE INDEX idx_write_commit_cdc_source
    ON write_commit_journal (cdc_stream_id, cdc_source_epoch)
    WHERE cdc_stream_id IS NOT NULL;

CREATE FUNCTION validate_write_cdc_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (
            OLD.cdc_stream_id IS DISTINCT FROM NEW.cdc_stream_id
            OR OLD.cdc_source_epoch IS DISTINCT FROM NEW.cdc_source_epoch
            OR OLD.cdc_offset_ranges IS DISTINCT FROM NEW.cdc_offset_ranges) THEN
        RAISE EXCEPTION 'CDC write identity is immutable';
    END IF;
    IF NEW.cdc_offset_ranges IS NULL THEN
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.cdc_offset_ranges) AS element
        WHERE jsonb_typeof(element) <> 'object'
            OR NOT (element ? 'topic' AND element ? 'partition'
                AND element ? 'startOffset' AND element ? 'endOffset')
            OR element - 'topic' - 'partition' - 'startOffset' - 'endOffset' <> '{}'::jsonb
            OR jsonb_typeof(element->'topic') <> 'string'
            OR jsonb_typeof(element->'partition') <> 'number'
            OR jsonb_typeof(element->'startOffset') <> 'number'
            OR jsonb_typeof(element->'endOffset') <> 'number'
            OR btrim(element->>'topic') = ''
            OR (element->>'partition')::numeric < 0
            OR (element->>'partition')::numeric > 2147483647
            OR (element->>'partition')::numeric <> trunc((element->>'partition')::numeric)
            OR (element->>'startOffset')::numeric < 0
            OR (element->>'startOffset')::numeric > 9223372036854775807
            OR (element->>'endOffset')::numeric <= (element->>'startOffset')::numeric
            OR (element->>'endOffset')::numeric > 9223372036854775807
    ) THEN
        RAISE EXCEPTION 'CDC offset range is not a bounded topic partition interval';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER write_commit_cdc_binding_guard
BEFORE INSERT OR UPDATE ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION validate_write_cdc_binding();
