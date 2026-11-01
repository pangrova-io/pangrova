package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactExecutionOwner;
import org.pangrova.security.store.PersistentDeferredAuthorization;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Process-local current authority evidence; neither a credential nor a persisted identity snapshot. */
public final class CurrentDeferredExecutionContext {
    private final ArtifactBuildDispatchSpec dispatchSpec;
    private final ExecutionAnchor executionAnchor;
    private final Set<String> currentDirectRoles;

    private CurrentDeferredExecutionContext(ArtifactBuildDispatchSpec dispatchSpec, ExecutionAnchor executionAnchor,
                                            Set<String> currentDirectRoles) {
        this.dispatchSpec = dispatchSpec;
        this.executionAnchor = executionAnchor;
        this.currentDirectRoles = Set.copyOf(currentDirectRoles);
    }

    /**
     * Resolves the actual tenant/member, current grants/models and exact backend execution owner on one connection.
     * The caller retains the transaction locks until dispatch preparation commits; renew and publish recheck authority.
     */
    public static CurrentDeferredExecutionContext resolve(ArtifactCurrentAuthority authority,
            Connection connection, String tenantId, String jobId,
            ArtifactExecutionOwner expectedOwner) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Deferred execution context requires the authority transaction");
        }
        var spec = authority.requireCurrent(connection, tenantId, jobId);
        var anchor = ArtifactExecutionFence.requireCurrent(connection, tenantId, jobId, expectedOwner);
        var roles = PersistentDeferredAuthorization.requireCurrentRoles(connection, spec.principal());
        return new CurrentDeferredExecutionContext(spec, anchor, roles);
    }

    /** Includes the original compiler-observed policies; current reads never invent new compiler observations. */
    public ArtifactBuildDispatchSpec dispatchSpec() { return dispatchSpec; }

    /** Actual backend-issued owner and bounded deadline, not a synthetic identity or caller token. */
    public ExecutionAnchor executionAnchor() { return executionAnchor; }

    /** Current persistent direct roles, distinct from IdP groups and never durably replayed as roles. */
    public Set<String> currentDirectRoles() { return currentDirectRoles; }
}
