-- The source factory identity is supplied by CREATE STREAM and retained by every recovery attempt.
ALTER TABLE ingestion_stream_state ADD COLUMN source_provider TEXT NOT NULL
    CHECK (source_provider ~ '^[a-z][a-z0-9_]*$');
