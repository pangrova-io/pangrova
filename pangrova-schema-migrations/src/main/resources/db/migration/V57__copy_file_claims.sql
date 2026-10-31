-- One durable admission for one tenant, Iceberg table incarnation, source URI,
-- and exact S3 object version. The application computes claim_key from those
-- four length-prefixed values and checks every returned row against the full
-- identity, so even a digest collision cannot alias different source files.
-- The content and profile evidence is immutable; a different execution cannot
-- take over an unresolved claim after timeout or coordinator restart.
CREATE TABLE copy_file_claims (
    claim_key CHAR(64) PRIMARY KEY CHECK (claim_key ~ '^[0-9a-f]{64}$'),
    claim_digest CHAR(64) NOT NULL CHECK (claim_digest ~ '^[0-9a-f]{64}$'),
    tenant_id TEXT NOT NULL CHECK (length(tenant_id) > 0),
    table_uuid UUID NOT NULL,
    source_url TEXT NOT NULL CHECK (length(source_url) > 0 AND octet_length(source_url) <= 1093),
    source_version_id TEXT NOT NULL CHECK (
        length(source_version_id) > 0 AND source_version_id <> 'null'
        AND octet_length(source_version_id) <= 1024),
    source_sha256 CHAR(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_size BIGINT NOT NULL CHECK (source_size >= 0),
    profile_name TEXT NOT NULL CHECK (length(profile_name) > 0),
    profile_incarnation_id UUID NOT NULL,
    profile_revision BIGINT NOT NULL CHECK (profile_revision > 0),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX copy_file_claims_execution ON copy_file_claims (tenant_id, execution_id);

-- A file remains unresolved until an Iceberg snapshot marker with this exact
-- claim_digest and the write-commit fingerprint is verified. That publication
-- transition is deliberately absent from this admission-only migration.
CREATE FUNCTION reject_copy_file_claim_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'COPY file claims are immutable; reconcile the Iceberg snapshot marker';
END;
$$;

CREATE TRIGGER copy_file_claims_no_update_or_delete
BEFORE UPDATE OR DELETE ON copy_file_claims
FOR EACH ROW EXECUTE FUNCTION reject_copy_file_claim_mutation();

CREATE TRIGGER copy_file_claims_no_truncate
BEFORE TRUNCATE ON copy_file_claims
FOR EACH STATEMENT EXECUTE FUNCTION reject_copy_file_claim_mutation();
