-- A source head, its accepted immutable manifest and the execution redo journal
-- are committed on the same metadata connection. No filesystem version tag is authority.
CREATE TABLE managed_source_schemas (
    catalog_name TEXT NOT NULL, catalog_incarnation TEXT NOT NULL,
    tenant_id TEXT NOT NULL, schema_name TEXT NOT NULL,
    PRIMARY KEY(catalog_name,catalog_incarnation,tenant_id,schema_name)
);
CREATE TABLE managed_source_heads (
    catalog_name TEXT NOT NULL, catalog_incarnation TEXT NOT NULL,
    tenant_id TEXT NOT NULL, schema_name TEXT NOT NULL, table_name TEXT NOT NULL,
    table_id TEXT NOT NULL, dataset_uri TEXT NOT NULL, generation BIGINT NOT NULL CHECK(generation>0),
    revision BIGINT NOT NULL CHECK(revision>0), visible_version BIGINT NOT NULL CHECK(visible_version>=0),
    row_count BIGINT NOT NULL CHECK(row_count>=0), schema_sha256 BYTEA NOT NULL CHECK(octet_length(schema_sha256)=32),
    payload BYTEA NOT NULL CHECK(octet_length(payload)>0 AND octet_length(payload)<=4194304),
    PRIMARY KEY(catalog_name,catalog_incarnation,tenant_id,schema_name,table_name),
    UNIQUE(catalog_name,catalog_incarnation,tenant_id,table_id),
    FOREIGN KEY(catalog_name,catalog_incarnation,tenant_id,schema_name)
      REFERENCES managed_source_schemas(catalog_name,catalog_incarnation,tenant_id,schema_name)
);
CREATE TABLE managed_source_publications (
    fingerprint TEXT PRIMARY KEY CHECK(fingerprint ~ '^[0-9a-f]{64}$'), execution_id TEXT NOT NULL UNIQUE,
    catalog_name TEXT NOT NULL, catalog_incarnation TEXT NOT NULL, catalog_revision BIGINT NOT NULL CHECK(catalog_revision>0),
    tenant_id TEXT NOT NULL, schema_name TEXT NOT NULL, table_name TEXT NOT NULL, table_id TEXT NOT NULL,
    dataset_uri TEXT NOT NULL, generation BIGINT NOT NULL CHECK(generation>0),
    base_revision BIGINT NOT NULL CHECK(base_revision>0), base_version BIGINT NOT NULL CHECK(base_version>=0),
    state TEXT NOT NULL CHECK(state IN ('PREPARING','PREPARED','ACCEPTED','ABORTED')),
    manifest_relative_path TEXT NOT NULL, published_version BIGINT NOT NULL CHECK(published_version>0),
    schema_sha256 BYTEA NOT NULL CHECK(octet_length(schema_sha256)=32),
    manifest_sha256 BYTEA, manifest_size BIGINT, naming_scheme TEXT, row_count BIGINT, payload BYTEA,
    inherited_membership TEXT REFERENCES managed_source_publications(fingerprint),
    object_count BIGINT, objects_sha256 BYTEA, registered_count BIGINT NOT NULL DEFAULT 0,
    next_object_page INTEGER NOT NULL DEFAULT 0 CHECK(next_object_page BETWEEN 0 AND 4096),
    intent_owner_id TEXT NOT NULL, intent_owner_incarnation TEXT NOT NULL, intent_owner_token BIGINT NOT NULL CHECK(intent_owner_token>0),
    CHECK(state <> 'ACCEPTED' OR (octet_length(manifest_sha256)=32 AND manifest_size>0
      AND naming_scheme IN ('V1','V2') AND row_count>=0 AND octet_length(payload)>0)),
    CHECK(state <> 'ABORTED' OR payload IS NULL),
    UNIQUE(catalog_name,catalog_incarnation,tenant_id,table_id,published_version,fingerprint)
);
-- Accepted history and uncertain candidates survive head drops and journal purge.
CREATE UNIQUE INDEX managed_source_accepted_versions ON managed_source_publications
    (catalog_name,catalog_incarnation,tenant_id,table_id,published_version) WHERE state='ACCEPTED';
CREATE TABLE managed_source_objects (
    fingerprint TEXT NOT NULL REFERENCES managed_source_publications(fingerprint),
    relative_path TEXT NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes>=0),
    purpose TEXT NOT NULL CHECK(purpose IN ('SOURCE','WINNER_SIDECAR')),
    cleanup_id TEXT, deleted_at TIMESTAMPTZ,
    PRIMARY KEY(fingerprint,relative_path)
);
CREATE TABLE managed_source_object_pages (
    fingerprint TEXT NOT NULL REFERENCES managed_source_publications(fingerprint),
    page_index INTEGER NOT NULL CHECK(page_index BETWEEN 0 AND 4095),
    after_path TEXT NOT NULL, last_path TEXT NOT NULL,
    object_count INTEGER NOT NULL CHECK(object_count BETWEEN 1 AND 512),
    page_sha256 BYTEA NOT NULL CHECK(octet_length(page_sha256)=32), is_last BOOLEAN NOT NULL,
    PRIMARY KEY(fingerprint,page_index)
);

-- Issuance precedes Native RPC; lost replies retain ISSUED regardless of lease expiry.
CREATE TABLE managed_source_prepare_invocations (
    invocation_id TEXT PRIMARY KEY CHECK(invocation_id ~ '^[0-9a-f]{32}$'),
    fingerprint TEXT NOT NULL REFERENCES managed_source_publications(fingerprint),
    execution_id TEXT NOT NULL, page_index INTEGER NOT NULL CHECK(page_index BETWEEN 0 AND 4095),
    after_path TEXT NOT NULL, worker_endpoint TEXT NOT NULL CHECK(length(worker_endpoint) BETWEEN 1 AND 2048),
    request_sha256 BYTEA NOT NULL CHECK(octet_length(request_sha256)=32),
    issued_owner_id TEXT NOT NULL, issued_owner_incarnation TEXT NOT NULL,
    issued_owner_token BIGINT NOT NULL CHECK(issued_owner_token>0),
    state TEXT NOT NULL CHECK(state IN ('ISSUED','DRAINED')),
    response_sha256 BYTEA, issued_at TIMESTAMPTZ NOT NULL, drained_at TIMESTAMPTZ,
    CHECK((state='ISSUED' AND response_sha256 IS NULL AND drained_at IS NULL)
      OR (state='DRAINED' AND octet_length(response_sha256)=32 AND drained_at IS NOT NULL))
);
CREATE INDEX managed_source_prepare_by_candidate ON managed_source_prepare_invocations(fingerprint,invocation_id);

CREATE TABLE managed_source_cleanup_batches (
    cleanup_id TEXT PRIMARY KEY CHECK(cleanup_id ~ '^[0-9a-f]{32}$'),
    fingerprint TEXT NOT NULL REFERENCES managed_source_publications(fingerprint),
    objects_sha256 BYTEA NOT NULL CHECK(octet_length(objects_sha256)=32),
    object_count INTEGER NOT NULL CHECK(object_count BETWEEN 1 AND 512),
    state TEXT NOT NULL CHECK(state IN ('DELETING','DELETED')),
    reserved_at TIMESTAMPTZ NOT NULL, deleted_at TIMESTAMPTZ,
    CHECK((state='DELETING' AND deleted_at IS NULL) OR (state='DELETED' AND deleted_at IS NOT NULL))
);
ALTER TABLE managed_source_objects ADD CONSTRAINT managed_source_cleanup_owner
    FOREIGN KEY(cleanup_id) REFERENCES managed_source_cleanup_batches(cleanup_id);
