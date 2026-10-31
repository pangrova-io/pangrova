package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.security.api.AccessDeniedException;

/** Binds builds to current grants and DROPPED cleanup to its exact persisted scope. */
public final class IndexSourceAuthority {
    private IndexSourceAuthority() {}

    /** Resolves the index's persisted subject with the authorization appropriate to its state. */
    public static ConnectorAccessorResolver forDescriptor(
            IndexMetadataStore indexes, IndexBuildServices builds,
            ConnectorAccessorResolver accessors, IndexDescriptor descriptor) {
        if (IndexDdlExecutor.requiresNativeArtifact(descriptor.indexType())) {
            if (builds.aiNative() == null) {
                throw new AccessDeniedException("Native index has no durable build authority");
            }
            return accessors.forSession(builds.aiNative().authorizedSession(descriptor));
        }
        String principal = descriptor.state() == IndexState.DROPPED
                ? indexes.loadDroppedIndexCleanupPrincipal(descriptor)
                : indexes.loadAuthorizedBuildPrincipal(descriptor);
        return accessors.forSession(DeferredIndexAuthorityResolver.resolveScalar(
                descriptor, principal).session());
    }
}
