-- Schema fingerprints before V124 were Java display-text digests. They cannot
-- be upgraded by rehashing the digest, and have no canonical version marker.
-- Quiesce old coordinators before applying this forward-only migration.
ALTER TABLE index_descriptors
    ALTER COLUMN source_schema_fingerprint TYPE VARCHAR(67);

-- Fail any staging generation bound to a legacy descriptor without disturbing
-- published generation/retention records or unresolved schema operation claims.
UPDATE index_generation_builds b
SET state='FAILED', failure_reason='source_schema_fingerprint_v2_required', updated_at=clock_timestamp()
WHERE b.state='BUILDING' AND EXISTS (
    SELECT 1 FROM index_descriptors d WHERE d.index_id=b.index_id
      AND (d.source_schema_fingerprint IS NULL OR d.source_schema_fingerprint !~ '^v2:[0-9a-f]{64}$')
);

-- Already STALE descriptors retain their exact timestamps and reason: a
-- pending V123 operation may own their captured descriptor fence. Their legacy
-- binding remains readable for cleanup but must be rebuilt before query use.
UPDATE index_descriptors
SET state='STALE', stale_reason='source_schema_fingerprint_v2_required', updated_at=clock_timestamp()
WHERE state IN ('READY','BUILDING')
  AND (source_schema_fingerprint IS NULL OR source_schema_fingerprint !~ '^v2:[0-9a-f]{64}$');
