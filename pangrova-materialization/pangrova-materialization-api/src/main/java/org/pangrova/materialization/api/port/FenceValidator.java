package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import org.pangrova.materialization.api.domain.ExecutionFence;

/**
 * Validates persisted execution fences against live coordinator ownership.
 */
@Experimental
public interface FenceValidator {

    /**
     * Requires that the fence is live, belongs to the current leader epoch, and
     * matches an existing execution anchor.
     *
     * @param fence fence to validate
     * @param now current instant
     */
    void requireValid(ExecutionFence fence, Instant now);
}
