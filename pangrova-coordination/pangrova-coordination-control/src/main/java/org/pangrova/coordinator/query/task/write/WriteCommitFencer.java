package org.pangrova.coordinator.query.task.write;

import org.pangrova.execution.api.ownership.ExecutionAnchor;

/**
 * Durable execution-ownership fence for the distributed-write commit path.
 *
 * <p>Singleton control leadership is unrelated to query ownership. A write may
 * commit only while the exact query anchor is still current in the shared store.</p>
 */
public interface WriteCommitFencer {

    /** Returns the locally owned anchor used to stamp a durable intent. */
    ExecutionAnchor currentAnchor(String executionId);

    /** Authoritatively validates the backend-issued token at commit time. */
    boolean isCurrent(String executionId, long fencingToken);
}
