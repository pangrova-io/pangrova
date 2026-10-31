package org.pangrova.generation.assembly;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.execution.GenerationStageGraph;
import org.pangrova.generation.protocol.GenerationStageGraphCodec;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

/** Admits a compiled graph before any durable build submission or transaction is started. */
@Internal
public final class GenerationDispatchSpecFactory {
    private GenerationDispatchSpecFactory() {}

    /** Validates structural resource/authority values and the exact encoded graph budget. */
    public static GenerationDispatchSpec create(String jobId, GenerationStageGraph stageGraph,
            String tenantId, String principal, DeferredAuthorizationScope authorizationScope,
            GenerationDispatchSpec.ResourceGrant resources) {
        var spec = new GenerationDispatchSpec(jobId, stageGraph, tenantId, principal, authorizationScope,
                resources.maxMemoryBytes(), resources.maxThreads(),
                resources.maxSpillBytes(), resources.taskTimeoutMillis());
        GenerationStageGraphCodec.encode(stageGraph);
        return spec;
    }
}
