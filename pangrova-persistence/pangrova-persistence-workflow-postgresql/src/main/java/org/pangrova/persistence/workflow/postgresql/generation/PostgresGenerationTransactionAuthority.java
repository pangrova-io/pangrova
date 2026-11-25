package org.pangrova.persistence.workflow.postgresql.generation;

import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.security.model.CurrentModelAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionInputs;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.persistence.jdbc.api.security.policy.AuthorizationPublicationLock;
import org.pangrova.persistence.security.api.tenant.DeferredTenantAuthority;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Bridges current security and canonical tenant authority on one metadata transaction. */
@org.pangrova.common.api.annotation.Internal
public final class PostgresGenerationTransactionAuthority implements GenerationTransactionAuthority {
    private final CurrentModelAuthority modelAuthority;
    private final GenerationTransactionInputs inputs;
    private final DeferredTenantAuthority tenantAuthority;
    private final AuthorizationPublicationLock publicationLock;
    private final TableControlWriteAuthority tableAuthority;

    /** Requires the real model owner to validate snapshots on the current transaction. */
    public PostgresGenerationTransactionAuthority(
            GenerationTransactionInputs inputs, CurrentModelAuthority modelAuthority,
            DeferredTenantAuthority tenantAuthority, AuthorizationPublicationLock publicationLock,
            TableControlWriteAuthority tableAuthority) {
        this.tableAuthority = java.util.Objects.requireNonNull(tableAuthority, "tableAuthority");
        this.inputs = java.util.Objects.requireNonNull(inputs, "inputs");
        this.modelAuthority = java.util.Objects.requireNonNull(modelAuthority, "modelAuthority");
        this.tenantAuthority = java.util.Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.publicationLock = java.util.Objects.requireNonNull(publicationLock, "publicationLock");
    }

    /** Loads the scoped dispatch specification and checks its current authority on this transaction. */
    @Override
    public AuthorizedDispatch authorize(Connection connection, String tenantId, String jobId) throws SQLException {
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
        return new AuthorizedDispatch(spec, requireScope(connection, spec));
    }

    /** Checks the current tenant, security, and model authority under the publication lock. */
    @Override
    public void requireCurrent(Connection connection, GenerationDispatchSpec spec) throws SQLException {
        publicationLock.lockPublication(connection);
        requireScope(connection, spec);
    }

    @Override
    public void requireCurrent(Connection connection, String tenantId, String principal,
                               DeferredAuthorizationScope authorizationScope) throws SQLException {
        publicationLock.lockPublication(connection);
        tenantAuthority.requireCurrent(connection, principal, tenantId, authorizationScope);
        tableAuthority.requireCurrent(connection, principal, authorizationScope);
        modelAuthority.requireCurrent(connection, principal, List.of());
    }

    private Set<String> requireScope(Connection connection, GenerationDispatchSpec spec) throws SQLException {
        tenantAuthority.requireCurrent(connection, spec.principal(), spec.tenantId(), spec.authorizationScope());
        Set<String> roles = tableAuthority.requireCurrent(connection, spec.principal(), spec.authorizationScope());
        modelAuthority.requireCurrent(connection, spec.principal(), spec.stageGraph().modelSnapshots());
        if (spec.authorizationScope().definitionSnapshot().isPresent()) {
            modelAuthority.requireCurrent(connection, spec.principal(),
                    spec.authorizationScope().definitionSnapshot().orElseThrow().models());
        }
        return roles;
    }
}
