package org.pangrova.databuild.assembly;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

/** Admits a compiled graph before any durable build submission or transaction is started. */
@Internal
public final class DataBuildDispatchSpecFactory {
    private DataBuildDispatchSpecFactory() {}

    /** Validates structural resource/authority values and the exact encoded graph budget. */
    public static DataBuildDispatchSpec create(String jobId, DataBuildStageGraph stageGraph,
            String tenantId, String principal, DeferredAuthorizationScope authorizationScope,
            DataBuildDispatchSpec.ResourceGrant resources) {
        var spec = new DataBuildDispatchSpec(jobId, stageGraph, tenantId, principal, authorizationScope,
                resources.maxMemoryBytes(), resources.maxThreads(),
                resources.maxSpillBytes(), resources.taskTimeoutMillis());
        DataBuildStageGraphCodec.encode(stageGraph);
        return spec;
    }
}
