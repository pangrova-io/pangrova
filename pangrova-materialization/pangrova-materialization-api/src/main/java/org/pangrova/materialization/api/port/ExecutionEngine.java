package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Executes a materialization plan in the native data plane.
 *
 * <p>The coordinator adapter submits the plan to a worker and returns only
 * exact commit evidence. Implementations must not expose rows, buffers, or
 * connector page sinks to the kernel.
 */
@Experimental
public interface ExecutionEngine {

    /**
     * Executes one attempt.
     *
     * @param request execution request
     * @return execution outcome with exact commit evidence
     */
    ExecutionResult execute(ExecutionRequest request);
}
