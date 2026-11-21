package org.pangrova.planner.api.enums;

import org.pangrova.common.api.annotation.Internal;

/**
 * Physical join strategies available for adaptive query execution.
 */
@Internal
public enum JoinStrategy {
    HASH_JOIN,
    BROADCAST_HASH_JOIN,
    SORT_MERGE_JOIN
}
