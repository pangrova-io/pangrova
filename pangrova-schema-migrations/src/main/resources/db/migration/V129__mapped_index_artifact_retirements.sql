-- Durable removal handoff for obsolete mapped scalar artifacts. Quiesce old writers
-- before applying: completed receipts permanently fence reuse of their physical paths.
CREATE SEQUENCE mapped_index_artifact_retirement_queue_seq AS BIGINT;

CREATE TABLE mapped_index_artifact_retirements (
    retirement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artifact_digest CHAR(64) NOT NULL UNIQUE,
    index_id VARCHAR(255) NOT NULL,
    descriptor_ref VARCHAR(255) REFERENCES index_descriptors(index_id) ON DELETE RESTRICT,
    tenant_id VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    data_file_path VARCHAR(2048) NOT NULL,
    artifact_path VARCHAR(2048) NOT NULL,
    retired_generation INT NOT NULL CHECK (retired_generation >= 0),
    state VARCHAR(16) NOT NULL CHECK (state IN ('PENDING', 'DELETED')),
    queue_order BIGINT NOT NULL DEFAULT nextval('mapped_index_artifact_retirement_queue_seq') CHECK (queue_order > 0),
    retired_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ,
    CHECK ((state = 'PENDING' AND descriptor_ref IS NOT NULL AND descriptor_ref = index_id AND deleted_at IS NULL)
        OR (state = 'DELETED' AND descriptor_ref IS NULL AND deleted_at IS NOT NULL))
);
ALTER SEQUENCE mapped_index_artifact_retirement_queue_seq
    OWNED BY mapped_index_artifact_retirements.queue_order;
CREATE INDEX idx_mapped_artifact_retirement_pending
    ON mapped_index_artifact_retirements(queue_order, retirement_id) WHERE state = 'PENDING';
CREATE INDEX idx_mapped_artifact_retirement_descriptor
    ON mapped_index_artifact_retirements(descriptor_ref) WHERE descriptor_ref IS NOT NULL;
CREATE INDEX idx_mapped_artifact_retirement_path ON mapped_index_artifact_retirements (md5(artifact_path));
CREATE INDEX idx_scalar_artifact_intents_path_digest ON scalar_index_artifact_intents (md5(artifact_path));

-- Same exact-path fence is taken by the transactional retirement producer.
-- Hash collisions serialize unrelated paths but never establish path equality.
CREATE FUNCTION pangrova_fence_retired_index_artifact() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE physical_path TEXT;
BEGIN
    IF TG_TABLE_NAME = 'index_file_mappings' THEN
        physical_path := NEW.index_file_path;
    ELSE
        physical_path := NEW.artifact_path;
    END IF;
    -- The lock must be followed by a fresh statement snapshot; the JDBC stores
    -- use READ COMMITTED. An older transaction snapshot cannot prove absence.
    IF current_setting('transaction_isolation') NOT IN ('read committed', 'read uncommitted') THEN
        RAISE EXCEPTION 'Index artifact path writes require READ COMMITTED isolation'
            USING ERRCODE = '25000';
    END IF;
    PERFORM pg_advisory_xact_lock(17002, hashtext(physical_path));
    IF EXISTS (SELECT 1 FROM mapped_index_artifact_retirements r
        WHERE md5(r.artifact_path) = md5(physical_path) AND r.artifact_path = physical_path) THEN
        RAISE EXCEPTION 'Retired index artifact path cannot be referenced again'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER index_file_mapping_retired_artifact_fence
    BEFORE INSERT OR UPDATE OF index_file_path ON index_file_mappings
    FOR EACH ROW EXECUTE FUNCTION pangrova_fence_retired_index_artifact();
CREATE TRIGGER scalar_artifact_intent_retired_artifact_fence
    BEFORE INSERT OR UPDATE OF artifact_path ON scalar_index_artifact_intents
    FOR EACH ROW EXECUTE FUNCTION pangrova_fence_retired_index_artifact();

-- Receipt identity and terminal deletion cannot be rewritten by a later update.
CREATE FUNCTION pangrova_preserve_mapped_artifact_retirement() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.retirement_id, NEW.artifact_digest, NEW.index_id, NEW.tenant_id,
           NEW.catalog_name, NEW.schema_name, NEW.table_name, NEW.data_file_path,
           NEW.artifact_path, NEW.retired_generation, NEW.retired_at)
       IS DISTINCT FROM
       ROW(OLD.retirement_id, OLD.artifact_digest, OLD.index_id, OLD.tenant_id,
           OLD.catalog_name, OLD.schema_name, OLD.table_name, OLD.data_file_path,
           OLD.artifact_path, OLD.retired_generation, OLD.retired_at)
       OR (OLD.state = 'DELETED' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'Mapped artifact retirement identity is immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER mapped_artifact_retirement_identity
    BEFORE UPDATE ON mapped_index_artifact_retirements
    FOR EACH ROW EXECUTE FUNCTION pangrova_preserve_mapped_artifact_retirement();
