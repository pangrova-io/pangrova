package org.pangrova.persistence.workflow.postgresql.security;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentDirectRoleReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentGrantReader;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentPolicyReader;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.security.authorization.deferred.DeferredAuthorization;
import org.pangrova.security.authorization.deferred.DeferredPolicyReader;
import org.pangrova.persistence.workflow.api.security.TableControlPolicyProjections;
import org.pangrova.persistence.workflow.api.security.TableControlWriteAuthority;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.PolicySnapshot;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.exception.AuthorizationDeniedException;

/**
 * Cross-domain same-connection write authority: binds Metadata-owned policy projections with
 * Table/Security persistence reads and Security-owned deferred evaluation.
 */
public final class PostgresTableControlWriteAuthority implements TableControlWriteAuthority {

    private final DeferredAuthorization authorization = new DeferredAuthorization(
            AuthorizationTransactionGate::lockPublication, new JdbcCurrentDirectRoleReader(),
                    new JdbcCurrentGrantReader());
    private final DeferredPolicyReader policies = new DeferredPolicyReader(new JdbcCurrentPolicyReader());
    private final TableControlPolicyProjections projections;

    public PostgresTableControlWriteAuthority(TableControlPolicyProjections projections) {
        this.projections = Objects.requireNonNull(projections, "projections");
    }

    @Override
    public Set<String> requireCurrent(Connection connection, String principal,
            DeferredAuthorizationScope scope) throws SQLException {
        return authorization.requireCurrent(connection, principal, scope, this::observe);
    }

    private PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
            TablePermission permission) throws SQLException {
        var table = permission.table();
        String denial = "Deferred table identity or catalog deployment changed";
        var identity = PostgresPolicyTableIdentity.find(connection, table.catalog(), table.schema(),
                table.table(), denial).orElse(null);
        if (identity != null && !identity.activeCatalog()) {
            throw new AuthorizationDeniedException(denial);
        }
        UUID tableId = identity == null ? null : identity.tableId();
        return policies.observe(connection, principal, roles, permission,
                () -> projections.projectRows(tableId, table.catalog(), table.schema(), table.table(),
                        JdbcRowPolicyStore.listForCurrentTable(connection, table.catalog(), table.schema(),
                                table.table(), tableId)),
                masks -> projections.projectColumns(tableId, table.catalog(), table.schema(), table.table(), masks,
                        tableId == null ? List.of() : PostgresTablePolicyRepository.list(connection, tableId)));
    }
}
