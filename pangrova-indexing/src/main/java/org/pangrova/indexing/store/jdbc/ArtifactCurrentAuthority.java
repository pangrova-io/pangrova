package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.DeferredTableControlAuthority;
import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.ai.store.jdbc.GeneratedModelCurrentAuthority;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** Bridges current security, table-control, and model authority on one transaction. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactCurrentAuthority {

    private final DeferredTableControlAuthority tableControl;

    /** The table-control owner implements and the composition root injects the metadata gate. */
    public ArtifactCurrentAuthority(DeferredTableControlAuthority tableControl) {
        this.tableControl = Objects.requireNonNull(tableControl,
                "tableControl must not be null");
    }

    /** Loads the scoped dispatch specification and checks its current authority on this transaction. */
    public ArtifactBuildDispatchSpec requireCurrent(Connection connection, String tenantId, String jobId)
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

    /** Checks the current tenant, security, table-control, and model authority under the publication lock. */
    public void requireCurrent(Connection connection, ArtifactBuildDispatchSpec spec) throws SQLException {
        AuthorizationTransactionGate.lockPublication(connection);
        JdbcTenantStore.requireDeferredTenant(connection, spec.principal(), spec.tenantId(), spec.authorizationScope());
        tableControl.requireCurrent(connection, spec.principal(), spec.authorizationScope());
        GeneratedModelCurrentAuthority.requireCurrent(connection, spec.principal(), spec.stageGraph().modelSnapshots());
        if (spec.authorizationScope().definitionSnapshot().isPresent()) {
            GeneratedModelCurrentAuthority.requireCurrent(connection, spec.principal(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
        }
    }
}
