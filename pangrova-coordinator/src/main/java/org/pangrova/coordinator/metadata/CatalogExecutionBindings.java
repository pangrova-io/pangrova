package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;

/** One canonical identity for a trusted catalog deployment, independent of read/write intent. */
public final class CatalogExecutionBindings {

    private CatalogExecutionBindings() {}

    /** Captures the exact unresolved configuration revision and incarnation. */
    public static CatalogExecutionBinding from(CatalogDescriptor descriptor) {
        return CatalogExecutionBinding.newBuilder()
                .setCatalogName(descriptor.name())
                .setConnectorProvider(descriptor.provider())
                .setCatalogIncarnationId(descriptor.incarnationId())
                .setCatalogRevision(descriptor.revision())
                .setConfigurationFingerprint(CatalogDeploymentFingerprint.checksum(
                        new CatalogDeploymentResponse(descriptor.name(), descriptor.provider(),
                                descriptor.incarnationId(), descriptor.revision(), descriptor.properties(),
                                descriptor.allowedTenants())))
                .build();
    }
}
