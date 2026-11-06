package org.pangrova.parser.api.ast;

/**
 * Typed classification of table references, replacing magic string comparisons
 * in TableRef resolution (e.g., "__derived__", "__lateral_derived__", "__lateral_func__").
 */
public enum TableRefKind {
    /** A named table reference (catalog.schema.table). */
    NAMED,
    /** A derived table reference (subquery in FROM clause). */
    DERIVED,
    /** An inline VALUES table constructor. */
    VALUES,
    /** A LATERAL derived table reference. */
    LATERAL_DERIVED,
    /** A LATERAL TABLE function reference (EXPLODE/POSEXPLODE). */
    LATERAL_FUNCTION,
    /** A system table reference (table$snapshots, table$files). */
    SYSTEM_TABLE,
    /** A PIVOT table reference (rows to columns). */
    PIVOT,
    /** An UNPIVOT table reference (columns to rows). */
    UNPIVOT
}
