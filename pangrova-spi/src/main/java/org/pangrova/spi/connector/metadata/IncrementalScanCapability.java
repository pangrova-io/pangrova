package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI contract declaring a connector's incremental scan capabilities.
 *
 * <p>Each connector (Iceberg, Paimon, etc.) implements this interface to
 * advertise which incremental scan modes it supports. The coordinator
 * uses these capabilities at Dynamic Table creation time to validate
 * the requested REFRESH_MODE and at refresh time to choose the
 * correct change detection strategy.</p>
 *
 * <h3>Connector implementation contract</h3>
 * <ul>
 *   <li>{@link #supportsIncrementalAppendScan()} — return {@code true}
 *       if the connector can enumerate newly added data files between
 *       two snapshots (e.g. Iceberg IncrementalAppendScan).</li>
 *   <li>{@link #supportsChangelogScan()} — return {@code true} if the
 *       connector natively produces a changelog stream with INSERT /
 *       UPDATE_BEFORE / UPDATE_AFTER / DELETE operations
 *       (e.g. Paimon changelog).</li>
 *   <li>{@link #isAppendOnly(String, long, long)} — inspect the snapshot
 *       range and return whether all changes are pure appends (no deletes
 *       or updates). Implementations must validate snapshot existence and
 *       connector-specific ancestry. Numeric snapshot ID ordering is not a
 *       portable ancestry test.</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe; the coordinator may invoke
 * these methods concurrently from different refresh tasks.</p>
 */
@Experimental
public interface IncrementalScanCapability {

    /**
     * Whether this connector supports incremental append scan.
     *
     * <p>When {@code true}, the change detector can request the set of
     * data files added between two snapshot IDs, enabling snapshot-diff
     * based incremental refresh.</p>
     *
     * @return {@code true} if incremental append scan is supported
     */
    boolean supportsIncrementalAppendScan();

    /**
     * Whether this connector supports changelog-based incremental scan.
     *
     * <p>When {@code true}, the change detector can consume a changelog
     * stream that includes INSERT, UPDATE_BEFORE, UPDATE_AFTER, and
     * DELETE operations, enabling fine-grained incremental refresh.</p>
     *
     * @return {@code true} if changelog scan is supported
     */
    boolean supportsChangelogScan();

    /**
     * Check whether all changes between two snapshots are append-only.
     *
     * <p>An append-only range contains only newly added data files with
     * no delete files (equality delete or position delete) and no
     * update operations. This determines whether the coordinator can
     * use the requested incremental path. An unprovable interval must be
     * rejected; it must not silently become a full refresh.</p>
     *
     * @param tableName      fully qualified table name
     * @param fromSnapshotId exclusive lower bound snapshot ID
     * @param toSnapshotId   inclusive upper bound snapshot ID
     * @return {@code true} if the range contains only appends
     * @throws IllegalArgumentException if an endpoint is invalid or the base is not an ancestor
     */
    boolean isAppendOnly(String tableName, long fromSnapshotId, long toSnapshotId);
}
