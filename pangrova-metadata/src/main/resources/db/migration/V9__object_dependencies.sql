-- ============================================================================
-- V9: Object dependency / lineage table.
--
-- Catalog now serves as the source of truth for inter-object dependencies
-- (e.g. view -> base tables, materialized view -> base tables, dynamic table
-- -> source streams). Used by:
--   - DROP enforcement (RESTRICT vs CASCADE policy on parent removal),
--   - Lineage queries (find dependents of a table; find references of a view),
--   - Future eager maintenance scheduling (snapshot expiry per dependent
--     coverage window — deferred, see G-CONN-07 / F-PAIMON-04).
--
-- Schema (G-META-08):
--   parent  = the object being depended on (a base table, source stream...)
--   child   = the dependent object (a view, MV, dynamic table...)
--   kind    = ObjectKind enum string (TABLE / VIEW / MATERIALIZED_VIEW /
--             DYNAMIC_TABLE / STREAM / FUNCTION / EXTERNAL)
--
-- Idempotent. PostgreSQL 15+ target; H2 (MODE=PostgreSQL) compatible.
-- ============================================================================

CREATE TABLE IF NOT EXISTS pangrova_object_dependencies (
    parent_catalog VARCHAR(255) NOT NULL,
    parent_schema  VARCHAR(255) NOT NULL,
    parent_object  VARCHAR(255) NOT NULL,
    parent_kind    VARCHAR(32)  NOT NULL,
    child_catalog  VARCHAR(255) NOT NULL,
    child_schema   VARCHAR(255) NOT NULL,
    child_object   VARCHAR(255) NOT NULL,
    child_kind     VARCHAR(32)  NOT NULL,
    created_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (parent_catalog, parent_schema, parent_object,
                 child_catalog, child_schema, child_object)
);

-- Forward lookup: "what does this child depend on?" (view -> base tables).
CREATE INDEX IF NOT EXISTS idx_obj_deps_child
    ON pangrova_object_dependencies (child_catalog, child_schema, child_object);

-- Reverse lookup: "who depends on this parent?" (drop enforcement, lineage).
CREATE INDEX IF NOT EXISTS idx_obj_deps_parent
    ON pangrova_object_dependencies (parent_catalog, parent_schema, parent_object);
