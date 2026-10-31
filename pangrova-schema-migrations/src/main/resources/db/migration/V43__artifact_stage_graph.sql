-- Executable graph metadata has one canonical format. Legacy flat payloads cannot
-- establish stage allocation or terminal coverage and must be explicitly rebuilt.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_build_dispatch_specs) THEN
        RAISE EXCEPTION 'Artifact stage graph migration requires explicit rebuild of existing flat dispatch specifications';
    END IF;
END $$;

ALTER TABLE artifact_build_dispatch_specs
    ADD COLUMN stage_graph_bytes BYTEA NOT NULL,
    ADD COLUMN stage_graph_sha256 VARCHAR(64) NOT NULL,
    DROP COLUMN plan_bytes,
    DROP COLUMN plan_sha256,
    DROP COLUMN operator_bindings_bytes,
    DROP COLUMN operator_bindings_sha256;

ALTER TABLE artifact_build_dispatch_specs ADD CONSTRAINT chk_artifact_stage_graph
    CHECK (octet_length(stage_graph_bytes) BETWEEN 1 AND 16777216
           AND stage_graph_sha256 ~ '^[0-9a-f]{64}$');

ALTER TABLE artifact_build_pages DROP CONSTRAINT chk_artifact_build_page_kind;
ALTER TABLE artifact_build_pages ADD CONSTRAINT chk_artifact_build_page_kind
    CHECK (page_kind IN ('SOURCE_MAP','DEPENDENCY_REDUCE','GENERATION_FINALIZE',
                        'RELATIONAL_COMPUTE','RELATIONAL_PARTITION'));
