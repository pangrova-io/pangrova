package org.pangrova.metadata.artifact.jdbc;

import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStoreMappings;

import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.store.PersistentDeferredAuthorization;
import org.pangrova.security.authz.AuthorizationDeniedException;

import java.sql.Connection;
import java.sql.SQLException;

/** Bridges current security and canonical tenant authority on one metadata transaction. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactCurrentAuthority {
    private ArtifactCurrentAuthority() {}

    /** Loads the scoped dispatch specification and checks its current authority on this transaction. */
    public static ArtifactBuildDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
            throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        ArtifactBuildDispatchSpec spec;
        try {
            spec = JdbcArtifactBuildStoreMappings.findDispatchSpec(connection, tenantId, jobId)
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
    public static void requireCurrent(Connection connection, ArtifactBuildDispatchSpec spec) throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        JdbcTenantStore.requireDeferredTenant(connection, spec.principal(), spec.tenantId(), spec.authorizationScope());
        PersistentDeferredAuthorization.requireCurrent(connection, spec.principal(), spec.authorizationScope());
        GeneratedModelCurrentAuthority.requireCurrent(connection, spec.principal(), spec.stageGraph().modelSnapshots());
        if (spec.authorizationScope().definitionSnapshot().isPresent()) {
            GeneratedModelCurrentAuthority.requireCurrent(connection, spec.principal(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
        }
    }
}
