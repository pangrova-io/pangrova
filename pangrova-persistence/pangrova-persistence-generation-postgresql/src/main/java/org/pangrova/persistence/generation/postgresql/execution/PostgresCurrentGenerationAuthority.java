package org.pangrova.persistence.generation.postgresql.execution;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;
import org.pangrova.persistence.generation.api.execution.CurrentGenerationAuthority;

import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/** Process-local current authority evidence; neither a credential nor a persisted identity snapshot. */
public final class PostgresCurrentGenerationAuthority implements CurrentGenerationAuthority {
    private final GenerationTransactionAuthority.AuthorizedDispatch authorization;
    private final ExecutionAnchor executionAnchor;

    private PostgresCurrentGenerationAuthority(GenerationTransactionAuthority.AuthorizedDispatch authorization,
            ExecutionAnchor executionAnchor) {
        this.authorization = authorization;
        this.executionAnchor = executionAnchor;
    }

    /**
     * Resolves the actual tenant/member, current grants/models and exact backend execution owner on one connection.
     * The caller retains the transaction locks until dispatch preparation commits; renew and publish recheck authority.
     */
    public static PostgresCurrentGenerationAuthority resolve(GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence, Connection connection, String tenantId, String jobId,
            GenerationExecutionOwner expectedOwner) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Deferred execution context requires the authority transaction");
        }
        var authorization = transactionAuthority.authorize(connection, tenantId, jobId);
        var spec = authorization.dispatchSpec();
        if (!tenantId.equals(spec.tenantId()) || !jobId.equals(spec.jobId())) {
            throw new IllegalArgumentException("Current artifact authority returned another tenant or job");
        }
        var anchor = executionFence.requireCurrent(connection, tenantId, jobId, expectedOwner);
        return new PostgresCurrentGenerationAuthority(authorization, anchor);
    }

    /** Includes the original compiler-observed policies; current reads never invent new compiler observations. */
    @Override
    public GenerationDispatchSpec dispatchSpec() { return authorization.dispatchSpec(); }

    /** Reuses the exact digest validated when this transaction loaded the original graph bytes. */
    public String graphSha256() { return authorization.stored().graphSha256(); }

    /** Actual backend-issued owner and bounded deadline, not a synthetic identity or caller token. */
    @Override
    public ExecutionAnchor executionAnchor() { return executionAnchor; }

    /** Current persistent direct roles, distinct from IdP groups and never durably replayed as roles. */
    @Override
    public Set<String> currentDirectRoles() { return authorization.currentDirectRoles(); }
}
