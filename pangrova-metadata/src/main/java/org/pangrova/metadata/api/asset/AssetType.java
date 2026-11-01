package org.pangrova.metadata.api.asset;

/**
 * Catalog-managed asset kind.
 *
 * <p>Pangrova's "universal catalog" surfaces multiple object types beyond plain
 * tables. Each {@link Asset} declares its kind via {@link Asset#type()} so a
 * single registry, listing, governance, and sync pipeline can fan out by type
 * instead of switching on instanceof at every call site.</p>
 */
public enum AssetType {

    /** A relational table (managed or external) with columns and partitioning. */
    TABLE,

    /** A logical SQL view: a named query rewritten on every reference. */
    VIEW,

    /** A materialized view: cached SQL result kept fresh by a refresh strategy. */
    MATERIALIZED_VIEW,

    /** A user-defined function (scalar or table-valued) registered in the catalog. */
    FUNCTION,

    /** A machine-learning model artifact addressable by name and version. */
    MODEL,

    /**
     * A volume / fileset: a named handle to a directory of files (parquet, CSV, JSON,
     * ML artifacts, configs, etc.) addressable from SQL without a relational schema.
     */
    VOLUME
}
