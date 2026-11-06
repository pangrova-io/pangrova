package org.pangrova.connector.api.index;

import org.pangrova.common.api.annotation.Experimental;

/** Causal relationship between two immutable snapshots from the same table. */
@Experimental
public enum SnapshotRelation {
    SAME,
    LEFT_ANCESTOR,
    RIGHT_ANCESTOR,
    DIVERGED
}
