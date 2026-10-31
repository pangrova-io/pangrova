-- A Dynamic Table definition and its published physical generation share one
-- row. A refresh first commits a separate physical table, then advances this
-- head with a revision compare-and-swap. Readers never observe a candidate.
CREATE TABLE dynamic_table_publications (
    tenant_id VARCHAR(255) NOT NULL,
    logical_name VARCHAR(512) NOT NULL,
    incarnation_id UUID NOT NULL,
    definition_sql TEXT NOT NULL,
    binding_catalog VARCHAR(255),
    binding_schema VARCHAR(255),
    published_table VARCHAR(512),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    lifecycle_state VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, logical_name),
    CHECK (lifecycle_state IN ('CREATING', 'ACTIVE')),
    CHECK ((lifecycle_state = 'CREATING' AND published_table IS NULL)
        OR (lifecycle_state = 'ACTIVE' AND published_table IS NOT NULL))
);

CREATE INDEX dynamic_table_publications_tenant_state
    ON dynamic_table_publications (tenant_id, lifecycle_state);
