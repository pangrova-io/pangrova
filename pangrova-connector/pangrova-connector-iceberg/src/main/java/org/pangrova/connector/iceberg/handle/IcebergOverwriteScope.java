package org.pangrova.connector.iceberg.handle;

/**
 * Declares exactly which rows an ICEBERG OVERWRITE allocation replaces.
 *
 * <p>The scope is captured when the write is planned and persisted in the output handle, so a
 * commit cannot silently widen a partition-scoped overwrite into a whole-table replacement, or
 * invent a whole-table replacement when the planner did not produce complete content.</p>
 */
public enum IcebergOverwriteScope {

    /** Not an overwrite (INSERT/DELETE) or a copy-on-write replacement, which is whole-table. */
    NONE,

    /** The allocation carries the complete replacement content for the table. */
    WHOLE_TABLE,

    /** The allocation carries the complete replacement content for the listed static partitions. */
    STATIC_PARTITION,

    /**
     * A dynamic partition overwrite needs the planner's affected-partition list. No planner
     * produces one today, so allocating this scope fails closed instead of replacing every
     * partition that received rows.
     */
    DYNAMIC_PARTITION
}
