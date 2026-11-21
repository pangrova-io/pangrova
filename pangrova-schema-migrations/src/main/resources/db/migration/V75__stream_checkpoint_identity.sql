-- Existing streams retain their durable checkpoint and default Kafka group key.
-- New creations supply an independent identity; never infer one from their name.
ALTER TABLE ingestion_stream_state ADD COLUMN checkpoint_id VARCHAR(255);
UPDATE ingestion_stream_state SET checkpoint_id = stream_name;
ALTER TABLE ingestion_stream_state ALTER COLUMN checkpoint_id SET NOT NULL;
ALTER TABLE ingestion_stream_state ADD CONSTRAINT stream_checkpoint_identity_unique UNIQUE (checkpoint_id);
