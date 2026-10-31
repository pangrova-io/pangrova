package org.pangrova.metadata.adapter.generation;

import org.pangrova.generation.spi.jdbc.GenerationTransactionAuthority;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;

import org.pangrova.generation.spi.jdbc.GenerationTransactionInputs;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.store.jdbc.AuthorizationTransactionGate;
import org.pangrova.security.api.AuthorizationDeniedException;

import java.sql.Connection;
import java.sql.SQLException;

/** Bridges current security and canonical tenant authority on one metadata transaction. */
@org.pangrova.common.api.annotation.Internal
public final class MetadataGenerationTransactionAuthority implements GenerationTransactionAuthority {
    private final CurrentModelAuthority modelAuthority;
    private final GenerationTransactionInputs inputs;

    /** Requires the real model owner to validate snapshots on the current transaction. */
    public MetadataGenerationTransactionAuthority(
            GenerationTransactionInputs inputs, CurrentModelAuthority modelAuthority) {
        this.inputs = java.util.Objects.requireNonNull(inputs, "inputs");
        this.modelAuthority = java.util.Objects.requireNonNull(modelAuthority, "modelAuthority");
    }

    /** Loads the scoped dispatch specification and checks its current authority on this transaction. */
    @Override
    public GenerationDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
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
        AuthorizationTransactionGate.lockPublication(connection);
        JdbcTenantStore.requireDeferredTenant(connection, spec.principal(), spec.tenantId(), spec.authorizationScope());
        org.pangrova.metadata.service.TableControlDeferredPolicy.requireCurrent(
                connection, spec.principal(), spec.authorizationScope());
        modelAuthority.requireCurrent(connection, spec.principal(), spec.stageGraph().modelSnapshots());
        if (spec.authorizationScope().definitionSnapshot().isPresent()) {
            modelAuthority.requireCurrent(connection, spec.principal(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
        }
    }
}
