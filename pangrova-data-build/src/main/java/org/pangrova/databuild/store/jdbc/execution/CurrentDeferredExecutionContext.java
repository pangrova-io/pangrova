package org.pangrova.databuild.store.jdbc.execution;

import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;

import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;
import org.pangrova.security.spi.jdbc.CurrentDirectRoleReader;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Process-local current authority evidence; neither a credential nor a persisted identity snapshot. */
public final class CurrentDeferredExecutionContext {
    private final DataBuildDispatchSpec dispatchSpec;
    private final ExecutionAnchor executionAnchor;
    private final Set<String> currentDirectRoles;

    private CurrentDeferredExecutionContext(DataBuildDispatchSpec dispatchSpec, ExecutionAnchor executionAnchor,
                                            Set<String> currentDirectRoles) {
        this.dispatchSpec = dispatchSpec;
        this.executionAnchor = executionAnchor;
        this.currentDirectRoles = Set.copyOf(currentDirectRoles);
    }

    /**
     * Resolves the actual tenant/member, current grants/models and exact backend execution owner on one connection.
     * The caller retains the transaction locks until dispatch preparation commits; renew and publish recheck authority.
     */
    public static CurrentDeferredExecutionContext resolve(DataBuildTransactionAuthority transactionAuthority,
            JdbcDataBuildExecutionFence executionFence, CurrentDirectRoleReader currentRoles,
            Connection connection, String tenantId, String jobId,
            DataBuildExecutionOwner expectedOwner) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Deferred execution context requires the authority transaction");
        }
        var spec = transactionAuthority.requireCurrent(connection, tenantId, jobId);
        if (!tenantId.equals(spec.tenantId()) || !jobId.equals(spec.jobId())) {
            throw new IllegalArgumentException("Current artifact authority returned another tenant or job");
        }
        var anchor = executionFence.requireCurrent(connection, tenantId, jobId, expectedOwner);
        var roles = currentRoles.requireCurrentRoles(connection, spec.principal());
        return new CurrentDeferredExecutionContext(spec, anchor, roles);
    }

    /** Includes the original compiler-observed policies; current reads never invent new compiler observations. */
    public DataBuildDispatchSpec dispatchSpec() { return dispatchSpec; }

    /** Actual backend-issued owner and bounded deadline, not a synthetic identity or caller token. */
    public ExecutionAnchor executionAnchor() { return executionAnchor; }

    /** Current persistent direct roles, distinct from IdP groups and never durably replayed as roles. */
    public Set<String> currentDirectRoles() { return currentDirectRoles; }
}
