package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Stable;

/**
 * Incremental change-set modes a connector may serve.
 */
@Stable(since = "1.0")
public enum ChangeSetMode {

    /** Only insertions occurred between the two snapshots. */
    APPEND_ONLY,

    /** Changes are confined to identifiable partitions that can be recomputed wholesale. */
    PARTITION_REPLACE,

    /** Changes affect identifiable deterministic aggregation keys. */
    AFFECTED_KEY,

    /** The connector can emit an ordered change-data stream. */
    CDC
}
