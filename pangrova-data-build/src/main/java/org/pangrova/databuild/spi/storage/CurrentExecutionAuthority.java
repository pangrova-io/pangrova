package org.pangrova.databuild.spi.storage;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import java.util.Set;

/**
 * Read-only evidence produced by a store after checking current domain authority and the actual execution owner.
 * This process-local view is not a credential, persisted identity or caller-supplied authorization input.
 * Every subsequent claim, renewal and publication must establish its own current authority.
 */
@Internal
public interface CurrentExecutionAuthority {
    /** Original compiler-observed policies validated against current domain authority. */
    DataBuildDispatchSpec dispatchSpec();

    /** Actual backend-issued execution owner and bounded deadline. */
    ExecutionAnchor executionAnchor();

    /** Current persistent direct roles; never an impersonated live session or replayed identity snapshot. */
    Set<String> currentDirectRoles();
}
