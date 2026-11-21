package org.pangrova.shuffle.exchange.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Partition merge strategies. A provider must explicitly support a strategy before using it. */
@Experimental(reason = "Server-side partition merging has no production caller")
public enum PartitionMergeStrategy {
    /** Keep producer fragments separate. */
    NONE,

    /** Concatenate committed fragments without sorting their rows. */
    APPEND,

    /** Merge sorted fragments by partition key. */
    SORT_MERGE
}
