ALTER TABLE worker_registry
    ADD COLUMN cdc_ingest_supported BOOLEAN NOT NULL DEFAULT FALSE;
