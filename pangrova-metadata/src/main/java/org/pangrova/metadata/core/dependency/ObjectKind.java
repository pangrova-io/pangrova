package org.pangrova.metadata.core.dependency;

/**
 * Categorization of catalog objects participating in a dependency edge.
 *
 * <p>Distinct from {@link org.pangrova.metadata.api.catalog.CatalogType}: that one
 * describes a whole catalog; this one describes individual objects inside a
 * catalog (the parent or child of a dependency edge).
 *
 * <p>Stored as a string in {@code pangrova_object_dependencies.{parent,child}_kind}
 * — adding a new kind requires no schema migration but does require updating
 * any consumer that does an exhaustive switch.
 */
public enum ObjectKind {

    /** Regular base table (Iceberg, Paimon, Hive, etc.). */
    TABLE,

    /** Logical, non-materialized view defined by a SQL query. */
    VIEW,

    /** Materialized view: a view whose result is physically stored. */
    MATERIALIZED_VIEW,

    /** Dynamic / continuously-refreshed table. */
    DYNAMIC_TABLE,

    /** Streaming source (e.g. Kafka topic, ingestion stream). */
    STREAM,

    /** Databricks-style managed or external file namespace. */
    VOLUME,

    /** Snowflake-style internal or external stage. */
    STAGE,

    /** User-defined function / scalar / table function. */
    FUNCTION,

    /**
     * External reference whose kind cannot be classified (e.g. a federation
     * target whose remote catalog does not expose object kinds).
     */
    EXTERNAL;

    /**
     * Returns true if dropping this kind without {@code CASCADE} should be
     * blocked when dependents exist.
     *
     * <p>Tables/views/MVs/dynamic-tables are protective by default; dropping
     * them silently would orphan downstream consumers. Streams/functions/external
     * are passive references and may be dropped independently.
     */
    public boolean isDropProtected() {
        return this == TABLE || this == VIEW || this == MATERIALIZED_VIEW || this == DYNAMIC_TABLE;
    }
}
