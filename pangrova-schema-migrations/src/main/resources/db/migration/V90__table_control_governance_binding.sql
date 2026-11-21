-- Governance bindings for controlled tables.
--
-- A binding is keyed by table_id, not by catalog.schema.table. Tags, masks, row
-- filters, quality rules, and lineage for an adopted or managed table therefore
-- cannot survive a drop/recreate that reuses the same name: the new table gets a
-- new table_id and the old bindings no longer match it.
--
-- Governance stores stay name-keyed for lookup on the read path; this table is
-- the authoritative record of which stable table a binding belongs to, and the
-- binder rejects a binding whose table_id no longer matches the live control
-- record.

CREATE TABLE IF NOT EXISTS table_control_governance_binding (
    binding_id    UUID         PRIMARY KEY,
    table_id      UUID         NOT NULL,
    binding_type  VARCHAR(32)  NOT NULL,
    binding_ref   VARCHAR(255) NOT NULL,
    config        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_by    VARCHAR(255) NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ  NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL,
    CONSTRAINT table_control_governance_binding_unique
        UNIQUE (table_id, binding_type, binding_ref),
    CONSTRAINT table_control_governance_binding_type_check
        CHECK (binding_type IN ('COLUMN_TAG', 'COLUMN_MASK', 'ROW_FILTER',
                                'QUALITY_RULE', 'LINEAGE'))
);

CREATE INDEX IF NOT EXISTS idx_table_control_governance_binding_table
    ON table_control_governance_binding (table_id, binding_type);
