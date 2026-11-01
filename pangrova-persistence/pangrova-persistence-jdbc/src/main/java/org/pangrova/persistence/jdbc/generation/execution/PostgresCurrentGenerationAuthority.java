package org.pangrova.persistence.jdbc.generation.execution;

import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationTransactionAuthority;
import org.pangrova.persistence.api.generation.transaction.jdbc.GenerationExecutionTransactionFence;
import org.pangrova.persistence.api.generation.execution.CurrentGenerationAuthority;

import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.execution.GenerationExecutionOwner;
import org.pangrova.persistence.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.spi.query.coordination.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Process-local current authority evidence; neither a credential nor a persisted identity snapshot. */
public final class PostgresCurrentGenerationAuthority implements CurrentGenerationAuthority {
    private final GenerationDispatchSpec dispatchSpec;
    private final ExecutionAnchor executionAnchor;
    private final Set<String> currentDirectRoles;

    private PostgresCurrentGenerationAuthority(GenerationDispatchSpec dispatchSpec, ExecutionAnchor executionAnchor,
                                            Set<String> currentDirectRoles) {
        this.dispatchSpec = dispatchSpec;
        this.executionAnchor = executionAnchor;
        this.currentDirectRoles = Set.copyOf(currentDirectRoles);
    }

    /**
     * Resolves the actual tenant/member, current grants/models and exact backend execution owner on one connection.
     * The caller retains the transaction locks until dispatch preparation commits; renew and publish recheck authority.
     */
    public static PostgresCurrentGenerationAuthority resolve(GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence, CurrentDirectRoleReader currentRoles,
            Connection connection, String tenantId, String jobId,
            GenerationExecutionOwner expectedOwner) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Deferred execution context requires the authority transaction");
        }
        var spec = transactionAuthority.requireCurrent(connection, tenantId, jobId);
        if (!tenantId.equals(spec.tenantId()) || !jobId.equals(spec.jobId())) {
            throw new IllegalArgumentException("Current artifact authority returned another tenant or job");
        }
        var anchor = executionFence.requireCurrent(connection, tenantId, jobId, expectedOwner);
        var roles = currentRoles.requireCurrentRoles(connection, spec.principal());
        return new PostgresCurrentGenerationAuthority(spec, anchor, roles);
    }

    /** Includes the original compiler-observed policies; current reads never invent new compiler observations. */
    @Override
    public GenerationDispatchSpec dispatchSpec() { return dispatchSpec; }

    /** Actual backend-issued owner and bounded deadline, not a synthetic identity or caller token. */
    @Override
    public ExecutionAnchor executionAnchor() { return executionAnchor; }

    /** Current persistent direct roles, distinct from IdP groups and never durably replayed as roles. */
    @Override
    public Set<String> currentDirectRoles() { return currentDirectRoles; }
}
