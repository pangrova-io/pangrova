package org.pangrova.planner.mv;

import java.util.OptionalLong;

/**
 * Provides snapshot information for merge-on-read resolution.
 *
 * <p>Implemented by the coordinator module to bridge between the planner's
 * DT resolution logic and the metadata/connector layer that tracks snapshot IDs.</p>
 *
 * <p>All methods are safe to call from the query planning hot path — implementations
 * should use cached values or fast lookups.</p>
 */
public interface SnapshotInfoProvider {

    /**
     * Returns the last refresh snapshot ID for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     * @return the snapshot ID up to which the DT is materialized, or empty if unknown
     */
    OptionalLong lastRefreshSnapshotId(String dtName);

    /**
     * Returns the source table name for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     * @return the source table name, or null if unknown
     */
    String sourceTable(String dtName);

    /**
     * Returns the current (latest) snapshot ID for a source table.
     *
     * @param sourceTable the source table name
     * @return the current snapshot ID, or empty if unknown
     */
    OptionalLong currentSnapshotId(String sourceTable);
}
