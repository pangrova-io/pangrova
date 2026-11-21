package org.pangrova.materialization.api.enums;

/**
 * Kind of materialized object. The kind selects lifecycle semantics and
 * rewrite participation, not a separate persistence path.
 */
public enum MaterializationKind {

    /** Dynamic Table: a managed, refreshable table definition. */
    DYNAMIC_TABLE,

    /** Materialized View: a definer-owned query result eligible for transparent rewrite. */
    MATERIALIZED_VIEW;

    /**
     * Returns whether this kind participates in transparent query rewrite.
     *
     * @return true for materialized views
     */
    public boolean participatesInTransparentRewrite() {
        return this == MATERIALIZED_VIEW;
    }
}
