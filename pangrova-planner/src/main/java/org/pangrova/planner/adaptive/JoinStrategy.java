package org.pangrova.planner.adaptive;

/**
 * Physical join strategies available for adaptive query execution.
 */
public enum JoinStrategy {
    HASH_JOIN,
    BROADCAST_HASH_JOIN,
    SORT_MERGE_JOIN
}
