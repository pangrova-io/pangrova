-- Each page carries only the selected certificates, in frozen incoming edge order.
ALTER TABLE artifact_build_pages
    ADD COLUMN exchange_batch_ranges BYTEA,
    ADD COLUMN adaptive_created BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT artifact_page_batch_ranges CHECK (
        exchange_batch_ranges IS NULL OR
        (page_kind = 'RELATIONAL_COMPUTE' AND exchange_partition_start IS NOT NULL
            AND exchange_partition_end - exchange_partition_start = 1
            AND octet_length(exchange_batch_ranges) IN (65,129))
    ),
    ADD CONSTRAINT artifact_adaptive_page_frozen CHECK (
        NOT adaptive_created OR (page_kind = 'RELATIONAL_COMPUTE' AND exchange_partition_start IS NOT NULL)
    );

DROP TRIGGER artifact_page_input_range_immutable ON artifact_build_pages;
CREATE TRIGGER artifact_page_input_range_immutable
BEFORE UPDATE OF exchange_partition_start, exchange_partition_end, exchange_batch_ranges, adaptive_created
ON artifact_build_pages
FOR EACH ROW WHEN (
    OLD.adaptive_created IS DISTINCT FROM NEW.adaptive_created OR
    (OLD.exchange_partition_start IS NOT NULL AND
        (OLD.exchange_partition_start IS DISTINCT FROM NEW.exchange_partition_start
            OR OLD.exchange_partition_end IS DISTINCT FROM NEW.exchange_partition_end
            OR OLD.exchange_batch_ranges IS DISTINCT FROM NEW.exchange_batch_ranges))
)
EXECUTE FUNCTION preserve_artifact_stage_ranges();
