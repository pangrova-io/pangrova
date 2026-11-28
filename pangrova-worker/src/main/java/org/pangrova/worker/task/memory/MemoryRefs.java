package org.pangrova.worker.task.memory;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.observability.api.tracing.TracingProvider;

/**
 * Groups memory and observability references for TaskManager.
 * Extracted from {@link TaskManagerContext} to comply with the ≤4 param governance limit.
 *
 * @param resourceManager unified resource lifecycle manager (memory, threads, spill)
 * @param nodeMemoryPool  hierarchical node memory pool (nullable — skip if null)
 * @param tracingProvider distributed tracing provider (nullable — skip if null)
 */
public record MemoryRefs(
        ResourceManager resourceManager,
        NodeMemoryPool nodeMemoryPool,
        TracingProvider tracingProvider) {

    public MemoryRefs {
        checkNotNull(resourceManager, "resourceManager must not be null");
    }

    /**
     * Convenience constructor without nullable fields.
     */
    public MemoryRefs(ResourceManager resourceManager) {
        this(resourceManager, null, null);
    }
}
