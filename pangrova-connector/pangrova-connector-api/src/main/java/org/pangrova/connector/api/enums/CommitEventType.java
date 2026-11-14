package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Classifies the type of data commit operation in a connector.
 *
 * <p>Used by {@code CommitEvent} to enable type-specific handling
 * in the indexing pipeline (e.g., invalidating mappings on overwrite,
 * marking indexes stale on MOR delete).</p>
 */
@Experimental
public enum CommitEventType {

    /** Pure INSERT — only new data files added. */
    APPEND(0),

    /** COW UPDATE/DELETE — both added and removed data files. */
    OVERWRITE(3),

    /** Compaction/clustering — data files rewritten without logical change. */
    COMPACT(1),

    /** MOR delete — delete files written but no data file replacement. */
    DELETE(4);

    private final int priority;

    CommitEventType(int priority) {
        this.priority = priority;
    }

    /**
     * Escalation priority for merging multiple commit events.
     * Higher value = more destructive operation = wins during merge.
     * Order: DELETE(4) > OVERWRITE(3) > COMPACT(1) > APPEND(0).
     */
    public int priority() {
        return priority;
    }
}
