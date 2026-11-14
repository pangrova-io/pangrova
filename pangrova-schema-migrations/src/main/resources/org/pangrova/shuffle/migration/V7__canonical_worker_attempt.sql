-- Legacy rows stay NULL: their lost process identity cannot be reconstructed.
-- Data access rejects these rows; lifecycle cleanup remains available.
ALTER TABLE shuffle_attempt_fence ADD COLUMN canonical_attempt BYTEA;
ALTER TABLE shuffle_producer_commit ADD COLUMN canonical_attempt BYTEA;
