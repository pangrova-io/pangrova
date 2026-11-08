package org.pangrova.metadata.generation;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.persistence.jdbc.api.security.policy.AuthorizationPublicationLock;
import org.pangrova.persistence.security.api.tenant.DeferredTenantAuthority;
import org.pangrova.security.api.error.AuthorizationDeniedException;

import java.sql.Connection;
import java.sql.SQLException;

/** Bridges current security and canonical tenant authority on one metadata transaction. */
@org.pangrova.common.api.annotation.Internal
public final class MetadataGenerationTransactionAuthority implements GenerationTransactionAuthority {
    private final CurrentModelAuthority modelAuthority;
    private final GenerationTransactionInputs inputs;
    private final DeferredTenantAuthority tenantAuthority;
    private final AuthorizationPublicationLock publicationLock;

    /** Requires the real model owner to validate snapshots on the current transaction. */
    public MetadataGenerationTransactionAuthority(
            GenerationTransactionInputs inputs, CurrentModelAuthority modelAuthority,
            DeferredTenantAuthority tenantAuthority, AuthorizationPublicationLock publicationLock) {
        this.inputs = java.util.Objects.requireNonNull(inputs, "inputs");
        this.modelAuthority = java.util.Objects.requireNonNull(modelAuthority, "modelAuthority");
        this.tenantAuthority = java.util.Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.publicationLock = java.util.Objects.requireNonNull(publicationLock, "publicationLock");
    }

    /** Loads the scoped dispatch specification and checks its current authority on this transaction. */
    @Override
    public GenerationDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException {
        publicationLock.lockPublication(connection);
        GenerationDispatchSpec spec;
        try {
            spec = inputs.findDispatchSpec(connection, tenantId, jobId)
                    .orElseThrow(() -> new AuthorizationDeniedException(
                            "Deferred work has no scoped dispatch specification"));
        } catch (IllegalArgumentException unscopedOrCorrupt) {
            throw new AuthorizationDeniedException(
                    "Deferred work has invalid scope metadata; securely resubmit the job");
        }
        requireCurrent(connection, spec);
        return spec;
    }

    /** Checks the current tenant, security, and model authority under the publication lock. */
    @Override
    public void requireCurrent(Connection connection, GenerationDispatchSpec spec) throws SQLException {
        publicationLock.lockPublication(connection);
        tenantAuthority.requireCurrent(connection, spec.principal(), spec.tenantId(), spec.authorizationScope());
        org.pangrova.metadata.service.TableControlWriteAuthorities.postgres().requireCurrent(
                connection, spec.principal(), spec.authorizationScope());
        modelAuthority.requireCurrent(connection, spec.principal(), spec.stageGraph().modelSnapshots());
        if (spec.authorizationScope().definitionSnapshot().isPresent()) {
            modelAuthority.requireCurrent(connection, spec.principal(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
        }
    }
}
